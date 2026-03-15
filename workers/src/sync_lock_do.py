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


class SyncCoordinator(DurableObject):
    """
    同期処理の同時実行を抑止する Durable Object ロック。
    - acquire: ロック要求（TTL付き）
    - release: ロック解放
    - status: 現在ロック状態の参照
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
            current["now"] = now
            return Response(
                json.dumps({"ok": True, "lock": current}, ensure_ascii=False),
                status=200,
                headers={"content-type": "application/json"},
            )
        
        # action が不明なら400を返す
        return Response(
            json.dumps({"ok": False, "error": "invalid_action"}, ensure_ascii=False),
            status=400,
            headers={"content-type": "application/json"},
        )
