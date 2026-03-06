import json
import time
from datetime import datetime, timezone


def _bool_env(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


class StateStore:
    def __init__(self, env):
        self.env = env

    def enabled(self) -> bool:
        return getattr(self.env, "STATE_KV", None) is not None

    def _kv(self):
        return getattr(self.env, "STATE_KV", None)

    async def get_text(self, key: str) -> str | None:
        kv = self._kv()
        if kv is None:
            return None
        value = await kv.get(key)
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    async def put_text(self, key: str, value: str):
        kv = self._kv()
        if kv is None:
            return
        await kv.put(key, str(value))

    async def get_json(self, key: str, default=None):
        text = await self.get_text(key)
        if not text:
            return default
        try:
            return json.loads(text)
        except Exception:
            return default

    async def put_json(self, key: str, payload):
        await self.put_text(
            key,
            json.dumps(payload, ensure_ascii=False),
        )

    async def mark_google_message_seen(self, channel_id: str, message_number: str) -> bool:
        """
        Returns True when the message was already seen, otherwise marks and returns False.
        """
        if not channel_id or not message_number:
            return False
        key = f"gcal_msg:{channel_id}:{message_number}"
        existing = await self.get_text(key)
        if existing is not None:
            return True
        await self.put_text(key, "1")
        return False

    async def get_sync_updated_min(self) -> str | None:
        return await self.get_text("sync:updated_min")

    async def set_sync_updated_min(self, updated_min: str):
        if updated_min:
            await self.put_text("sync:updated_min", str(updated_min))

    async def get_sync_last_epoch(self) -> float:
        text = await self.get_text("sync:last_epoch")
        if not text:
            return 0.0
        try:
            return float(text)
        except Exception:
            return 0.0

    async def set_sync_last_epoch_now(self):
        await self.put_text("sync:last_epoch", str(time.time()))

    async def should_skip_sync_by_cooldown(self, interval_seconds: float) -> bool:
        if interval_seconds <= 0:
            return False
        now = time.time()
        last_epoch = await self.get_sync_last_epoch()
        return (now - last_epoch) < interval_seconds

    async def get_gcal_discord_map(self) -> dict:
        value = await self.get_json("map:gcal_discord", {})
        return value if isinstance(value, dict) else {}

    async def set_gcal_discord_map(self, data: dict):
        await self.put_json("map:gcal_discord", data or {})

    async def get_gcal_notion_map(self) -> dict:
        value = await self.get_json("map:gcal_notion", {"internal": {}, "external": {}})
        if not isinstance(value, dict):
            return {"internal": {}, "external": {}}
        value.setdefault("internal", {})
        value.setdefault("external", {})
        return value

    async def set_gcal_notion_map(self, data: dict):
        payload = data if isinstance(data, dict) else {"internal": {}, "external": {}}
        payload.setdefault("internal", {})
        payload.setdefault("external", {})
        await self.put_json("map:gcal_notion", payload)

    async def get_discord_snapshot(self) -> dict:
        value = await self.get_json("discord:snapshot", {})
        return value if isinstance(value, dict) else {}

    async def set_discord_snapshot(self, data: dict):
        await self.put_json("discord:snapshot", data or {})

    async def set_last_result(self, op_name: str, payload: dict):
        if not op_name:
            return
        now_iso = datetime.now(timezone.utc).isoformat()
        data = {
            "updated_at": now_iso,
            "payload": payload or {},
        }
        await self.put_json(f"result:{op_name}", data)

    async def get_last_result(self, op_name: str):
        if not op_name:
            return None
        value = await self.get_json(f"result:{op_name}", None)
        return value if isinstance(value, dict) else None

    @staticmethod
    def is_kv_sync_cooldown_enabled(env) -> bool:
        return _bool_env(getattr(env, "KV_SYNC_COOLDOWN_ENABLED", "true"), default=True)

    @staticmethod
    def is_gcal_dedupe_enabled(env) -> bool:
        return _bool_env(getattr(env, "KV_GCAL_DEDUPE_ENABLED", "true"), default=True)
