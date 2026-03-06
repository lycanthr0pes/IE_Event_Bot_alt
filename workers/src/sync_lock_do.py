import json
import time
from uuid import uuid4

from workers import DurableObject, Response


class SyncCoordinator(DurableObject):
    async def fetch(self, request):
        try:
            payload = json.loads(await request.text() or "{}")
        except Exception:
            payload = {}

        action = str(payload.get("action") or "").strip().lower()
        now = time.time()

        if action == "acquire":
            owner = str(payload.get("owner") or f"owner-{uuid4()}")
            ttl_seconds = float(payload.get("ttl_seconds") or 30)
            expires_at = now + max(1.0, ttl_seconds)
            current = await self.ctx.storage.get("lock")
            if isinstance(current, dict):
                current_owner = str(current.get("owner") or "")
                current_exp = float(current.get("expires_at") or 0)
                if current_exp > now and current_owner and current_owner != owner:
                    return Response(
                        json.dumps(
                            {"ok": False, "locked": True, "owner": current_owner},
                            ensure_ascii=False,
                        ),
                        status=409,
                        headers={"content-type": "application/json"},
                    )
            await self.ctx.storage.put("lock", {"owner": owner, "expires_at": expires_at})
            return Response(
                json.dumps({"ok": True, "owner": owner, "expires_at": expires_at}, ensure_ascii=False),
                status=200,
                headers={"content-type": "application/json"},
            )

        if action == "release":
            owner = str(payload.get("owner") or "")
            current = await self.ctx.storage.get("lock")
            if isinstance(current, dict):
                current_owner = str(current.get("owner") or "")
                if not owner or owner == current_owner:
                    await self.ctx.storage.delete("lock")
            return Response(
                json.dumps({"ok": True}, ensure_ascii=False),
                status=200,
                headers={"content-type": "application/json"},
            )

        if action == "status":
            current = await self.ctx.storage.get("lock")
            if not isinstance(current, dict):
                current = {}
            current["now"] = now
            return Response(
                json.dumps({"ok": True, "lock": current}, ensure_ascii=False),
                status=200,
                headers={"content-type": "application/json"},
            )

        return Response(
            json.dumps({"ok": False, "error": "invalid_action"}, ensure_ascii=False),
            status=400,
            headers={"content-type": "application/json"},
        )
