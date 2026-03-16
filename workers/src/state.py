import json
import time
from datetime import datetime, timezone


def _bool_env(value: str | None, default: bool = False) -> bool:
    """環境変数文字列を bool として解釈する。"""
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _json_text(payload) -> str:
    """JSON 比較/保存用に安定した文字列表現へ正規化する。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class StateStore:
    """
    Workers KV 上の状態アクセスを集約する。
    - 同期カーソル/最終実行時刻の保持
    - Google webhook 重複通知の抑止
    - gcal<->discord/notion マッピング保持
    - ジョブ結果の保存
    """

    def __init__(self, env):
        self.env = env

    def enabled(self) -> bool:
        """STATE_KV バインディングの有無を返す。"""
        return getattr(self.env, "STATE_KV", None) is not None

    def _kv(self):
        """内部ヘルパー: KV バインディングを返す。"""
        return getattr(self.env, "STATE_KV", None)

    def _sync_do(self):
        """内部ヘルパー: SyncCoordinator Durable Object namespace を返す。"""
        return getattr(self.env, "SYNC_COORDINATOR", None)

    @staticmethod
    def _sync_do_stub(do_ns):
        """Durable Object namespace から global stub を取得する。"""
        if do_ns is None:
            return None
        if hasattr(do_ns, "get_by_name"):
            return do_ns.get_by_name("global")
        if hasattr(do_ns, "id_from_name") and hasattr(do_ns, "get"):
            return do_ns.get(do_ns.id_from_name("global"))
        if hasattr(do_ns, "idFromName") and hasattr(do_ns, "get"):
            return do_ns.get(do_ns.idFromName("global"))
        return None

    @staticmethod
    async def _sync_do_fetch(stub, action: str, payload: dict | None = None):
        """SyncCoordinator へ JSON POST し、結果辞書を返す。"""
        if stub is None:
            return None
        body = _json_text({"action": action, **(payload or {})})
        try:
            response = await stub.fetch(
                "https://sync-lock/internal",
                method="POST",
                headers={"content-type": "application/json"},
                body=body,
            )
        except TypeError:
            response = await stub.fetch(
                "https://sync-lock/internal",
                {
                    "method": "POST",
                    "headers": {"content-type": "application/json"},
                    "body": body,
                },
            )
        text = await response.text()
        try:
            return json.loads(text or "{}")
        except Exception:
            return {}

    async def get_text(self, key: str) -> str | None:
        """KV から文字列を取得し、空文字は None として扱う。"""
        kv = self._kv()
        if kv is None:
            return None
        value = await kv.get(key)
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    async def put_text(self, key: str, value: str):
        """KV へ文字列を書き込む。"""
        kv = self._kv()
        if kv is None:
            return
        await kv.put(key, str(value))

    async def put_text_if_changed(self, key: str, value: str) -> bool:
        """現在値と異なる場合だけ KV へ文字列を書き込む。"""
        next_value = str(value)
        current = await self.get_text(key)
        if current == next_value:
            return False
        await self.put_text(key, next_value)
        return True

    async def get_json(self, key: str, default=None):
        """KV の JSON 文字列を辞書等へ復元する。失敗時は default。"""
        text = await self.get_text(key)
        if not text:
            return default
        try:
            return json.loads(text)
        except Exception:
            return default

    async def put_json(self, key: str, payload):
        """Python オブジェクトを JSON 化して KV へ保存する。"""
        await self.put_text(
            key,
            _json_text(payload),
        )

    async def put_json_if_changed(self, key: str, payload) -> bool:
        """現在値と異なる場合だけ JSON を KV へ保存する。"""
        next_text = _json_text(payload)
        current = await self.get_text(key)
        if current == next_text:
            return False
        await self.put_text(key, next_text)
        return True

    async def mark_google_message_seen(self, channel_id: str, message_number: str) -> bool:
        """
        Google webhook 重複通知抑止用。
        返り値:
            True: 既に処理済み
            False: 未処理だったので今回マークした
        """
        if not channel_id or not message_number:
            return False
        do_ns = self._sync_do()
        if do_ns is not None:
            stub = self._sync_do_stub(do_ns)
            result = await self._sync_do_fetch(
                stub,
                "mark_google_message_seen",
                {
                    "channel_id": channel_id,
                    "message_number": message_number,
                    "ttl_seconds": self.google_message_dedupe_ttl_seconds(self.env),
                },
            )
            if isinstance(result, dict) and "duplicate" in result:
                return bool(result.get("duplicate"))
        key = f"gcal_msg:{channel_id}:{message_number}"
        # 存在確認
        existing = await self.get_text(key)
        if existing is not None:
            return True
        await self.put_text(key, "1")
        return False

    async def get_sync_updated_min(self) -> str | None:
        """Google差分同期カーソル(updatedMin)を取得する。"""
        return await self.get_text("sync:updated_min")

    async def set_sync_updated_min(self, updated_min: str):
        """Google差分同期カーソル(updatedMin)を保存する。"""
        if updated_min:
            await self.put_text_if_changed("sync:updated_min", str(updated_min))

    async def get_sync_last_epoch(self) -> float:
        """最後に同期成功した時刻(epoch秒)を取得する。"""
        do_ns = self._sync_do()
        if do_ns is not None:
            stub = self._sync_do_stub(do_ns)
            result = await self._sync_do_fetch(stub, "get_sync_last_epoch")
            try:
                return float((result or {}).get("last_epoch") or 0.0)
            except Exception:
                return 0.0
        text = await self.get_text("sync:last_epoch")
        if not text:
            return 0.0
        try:
            return float(text)
        except Exception:
            return 0.0

    async def set_sync_last_epoch_now(self):
        """最後の同期時刻を現在時刻で更新する。"""
        now_epoch = time.time()
        do_ns = self._sync_do()
        if do_ns is not None:
            stub = self._sync_do_stub(do_ns)
            await self._sync_do_fetch(stub, "set_sync_last_epoch", {"last_epoch": now_epoch})
            return
        await self.put_text("sync:last_epoch", str(now_epoch))

    async def should_skip_sync_by_cooldown(self, interval_seconds: float) -> bool:
        """クールダウン判定。直近実行から interval 未満なら True。"""
        if interval_seconds <= 0:
            return False
        now = time.time()
        last_epoch = await self.get_sync_last_epoch()
        return (now - last_epoch) < interval_seconds

    async def get_gcal_discord_map(self) -> dict:
        """GoogleイベントID -> DiscordイベントID の対応表を取得する。"""
        value = await self.get_json("map:gcal_discord", {})
        return value if isinstance(value, dict) else {}

    async def set_gcal_discord_map(self, data: dict):
        """GoogleイベントID -> DiscordイベントID の対応表を保存する。"""
        await self.put_json_if_changed("map:gcal_discord", data or {})

    async def get_gcal_notion_map(self) -> dict:
        """GoogleイベントID -> NotionページID の対応表を取得する。"""
        value = await self.get_json("map:gcal_notion", {"internal": {}, "external": {}})
        if not isinstance(value, dict):
            return {"internal": {}, "external": {}}
        value.setdefault("internal", {})
        value.setdefault("external", {})
        return value

    async def set_gcal_notion_map(self, data: dict):
        """GoogleイベントID -> NotionページID の対応表を保存する。"""
        payload = data if isinstance(data, dict) else {"internal": {}, "external": {}}
        payload.setdefault("internal", {})
        payload.setdefault("external", {})
        await self.put_json_if_changed("map:gcal_notion", payload)

    async def get_discord_snapshot(self) -> dict:
        """Discordポーリング差分検知用スナップショットを取得する。"""
        value = await self.get_json("discord:snapshot", {})
        return value if isinstance(value, dict) else {}

    async def set_discord_snapshot(self, data: dict):
        """Discordポーリング差分検知用スナップショットを保存する。"""
        await self.put_json_if_changed("discord:snapshot", data or {})

    async def set_last_result(self, op_name: str, payload: dict):
        """ジョブ/同期結果を `result:<op_name>` に保存する。"""
        if not op_name:
            return
        existing = await self.get_last_result(op_name)
        min_interval = self.result_write_min_interval_seconds(self.env)
        if isinstance(existing, dict):
            existing_payload = existing.get("payload") or {}
            existing_updated_at = str(existing.get("updated_at") or "")
            try:
                existing_dt = datetime.fromisoformat(existing_updated_at.replace("Z", "+00:00"))
            except Exception:
                existing_dt = None
            if existing_payload == (payload or {}) and existing_dt is not None:
                elapsed = (datetime.now(timezone.utc) - existing_dt).total_seconds()
                if elapsed < min_interval:
                    return
        now_iso = datetime.now(timezone.utc).isoformat()
        data = {
            "updated_at": now_iso,
            "payload": payload or {},
        }
        await self.put_json_if_changed(f"result:{op_name}", data)

    async def get_last_result(self, op_name: str):
        """`result:<op_name>` の最新結果を取得する。"""
        if not op_name:
            return None
        value = await self.get_json(f"result:{op_name}", None)
        return value if isinstance(value, dict) else None

    @staticmethod
    def result_write_min_interval_seconds(env) -> float:
        """同一内容の last_result を再保存する最小間隔を返す。"""
        raw = getattr(env, "KV_RESULT_MIN_WRITE_SECONDS", "3600")
        try:
            return max(0.0, float(raw))
        except Exception:
            return 3600.0

    @staticmethod
    def google_message_dedupe_ttl_seconds(env) -> float:
        """Google webhook 重複抑止の保持秒数を返す。"""
        raw = getattr(env, "GCAL_DEDUPE_TTL_SECONDS", "86400")
        try:
            return max(60.0, float(raw))
        except Exception:
            return 86400.0

    @staticmethod
    def is_kv_sync_cooldown_enabled(env) -> bool:
        """同期クールダウン機能の有効/無効を返す。"""
        return _bool_env(getattr(env, "KV_SYNC_COOLDOWN_ENABLED", "true"), default=True)

    @staticmethod
    def is_gcal_dedupe_enabled(env) -> bool:
        """Google webhook 重複抑止機能の有効/無効を返す。"""
        return _bool_env(getattr(env, "KV_GCAL_DEDUPE_ENABLED", "true"), default=True)
