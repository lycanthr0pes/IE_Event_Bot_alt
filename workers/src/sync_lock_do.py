import json
import time
from uuid import uuid4

from workers import DurableObject, Response


def _decode_lock_record(value) -> dict:
    """
    storage.get("lock") の返り値を lock 辞書へ正規化する。
    - 新形式: JSON文字列
    - 旧形式: dict（互換）
    """
    # 文字列ならJSONとして読む
    if isinstance(value, str):
        try:
            data = json.loads(value or "{}")
        except Exception:
            data = {}
    # 辞書ならそのまま使う
    elif isinstance(value, dict):
        data = value
    else:
        data = {}
    if not isinstance(data, dict):
        return {}
    return {
        "owner": str(data.get("owner") or ""),
        "expires_at": float(data.get("expires_at") or 0),
    }


def _decode_json_record(value) -> dict:
    """storage 上の JSON 文字列/辞書を dict に正規化する。"""
    if isinstance(value, str):
        try:
            data = json.loads(value or "{}")
        except Exception:
            data = {}
    elif isinstance(value, dict):
        data = value
    else:
        data = {}
    return data if isinstance(data, dict) else {}


class SyncCoordinator(DurableObject):
    """
    同期処理まわりの高頻度状態を扱う Durable Object。
    - acquire: ロック要求（TTL付き）
    - release: ロック解放
    - status: 現在ロック状態の参照
    - get/set_sync_last_epoch: 同期成功時刻
    - mark_google_message_seen: Google webhook 重複抑止
    """

    async def fetch(self, request):
        """
        POST body(JSON) の action に応じてロック操作を実行する。
        """
        try:
            payload = json.loads(await request.text() or "{}")
        except Exception:
            payload = {}

        # action と現在時刻を取得
        action = str(payload.get("action") or "").strip().lower()
        now = time.time()

        # ロック要求処理
        if action == "acquire":
            # 既存ロックが有効かつ他owner保有中なら 409 で拒否。
            owner = str(payload.get("owner") or f"owner-{uuid4()}")
            # ロックの有効秒数を決める
            ttl_seconds = float(payload.get("ttl_seconds") or 30)
            expires_at = now + max(1.0, ttl_seconds)
            # 現在のロック情報を読む(Durable Object のストレージから取得)
            current = _decode_lock_record(await self.ctx.storage.get("lock"))
            if current:
                current_owner = str(current.get("owner") or "")
                current_exp = float(current.get("expires_at") or 0)
                """
                ロック拒否条件(他人がロック中):
                - まだ期限切れしていない
                - owner が存在する
                - 今の要求者とは別 owner
                """
                if current_exp > now and current_owner and current_owner != owner:
                    return Response(
                        json.dumps(
                            {"ok": False, "locked": True, "owner": current_owner},
                            ensure_ascii=False,
                        ),
                        status=409,
                        headers={"content-type": "application/json"},
                    )
            # 他人の有効ロックが無ければ、自分のロック情報を書き込む
            # Python Workers の DO storage は dict 直putで DataCloneError になる場合があるため文字列化して保存
            await self.ctx.storage.put(
                "lock",
                json.dumps({"owner": owner, "expires_at": expires_at}, ensure_ascii=False),
            )
            return Response(
                json.dumps({"ok": True, "owner": owner, "expires_at": expires_at}, ensure_ascii=False),
                status=200,
                headers={"content-type": "application/json"},
            )

        # ロック解放処理
        if action == "release":
            # owner 未指定なら強制解放、owner 指定時は一致する(自分のロック)場合のみ解放。
            owner = str(payload.get("owner") or "")
            current = _decode_lock_record(await self.ctx.storage.get("lock"))
            if current:
                current_owner = str(current.get("owner") or "")
                if not owner or owner == current_owner:
                    await self.ctx.storage.delete("lock")
            return Response(
                json.dumps({"ok": True}, ensure_ascii=False),
                status=200,
                headers={"content-type": "application/json"},
            )

        if action == "status":
            # 監視用途。現在 lock と現在時刻(now)を返す。
            current = _decode_lock_record(await self.ctx.storage.get("lock"))
            sync_state = _decode_json_record(await self.ctx.storage.get("sync:last_epoch"))
            current["now"] = now
            return Response(
                json.dumps(
                    {
                        "ok": True,
                        "lock": current,
                        "sync_last_epoch": float(sync_state.get("last_epoch") or 0.0),
                    },
                    ensure_ascii=False,
                ),
                status=200,
                headers={"content-type": "application/json"},
            )

        # 同期成功時刻の取得 (CD用)
        if action == "get_sync_last_epoch":
            state = _decode_json_record(await self.ctx.storage.get("sync:last_epoch"))
            return Response(
                json.dumps(
                    {"ok": True, "last_epoch": float(state.get("last_epoch") or 0.0)},
                    ensure_ascii=False,
                ),
                status=200,
                headers={"content-type": "application/json"},
            )

        # 同期成功時刻の更新 (CD用)
        if action == "set_sync_last_epoch":
            last_epoch = float(payload.get("last_epoch") or now)
            await self.ctx.storage.put(
                "sync:last_epoch",
                json.dumps({"last_epoch": last_epoch}, ensure_ascii=False),
            )
            return Response(
                json.dumps({"ok": True, "last_epoch": last_epoch}, ensure_ascii=False),
                status=200,
                headers={"content-type": "application/json"},
            )

        if action == "mark_google_message_seen":
            channel_id = str(payload.get("channel_id") or "").strip()
            message_number = str(payload.get("message_number") or "").strip()
            if not channel_id or not message_number:
                return Response(
                    json.dumps({"ok": True, "duplicate": False, "skipped": True}, ensure_ascii=False),
                    status=200,
                    headers={"content-type": "application/json"},
                )
            ttl_seconds = max(60.0, float(payload.get("ttl_seconds") or 86400))
            storage_key = f"gcal_msg:{channel_id}:{message_number}"
            current = _decode_json_record(await self.ctx.storage.get(storage_key))
            expires_at = float(current.get("expires_at") or 0.0)
            if expires_at > now:
                return Response(
                    json.dumps({"ok": True, "duplicate": True, "expires_at": expires_at}, ensure_ascii=False),
                    status=200,
                    headers={"content-type": "application/json"},
                )
            next_expires_at = now + ttl_seconds
            await self.ctx.storage.put(
                storage_key,
                json.dumps({"expires_at": next_expires_at}, ensure_ascii=False),
            )
            return Response(
                json.dumps(
                    {"ok": True, "duplicate": False, "expires_at": next_expires_at},
                    ensure_ascii=False,
                ),
                status=200,
                headers={"content-type": "application/json"},
            )
        
        # action が不明なら400を返す
        return Response(
            json.dumps({"ok": False, "error": "invalid_action"}, ensure_ascii=False),
            status=400,
            headers={"content-type": "application/json"},
        )
