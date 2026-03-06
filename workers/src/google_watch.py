import json
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING
from urllib.parse import quote
from uuid import uuid4

from google_auth import get_google_access_token

if TYPE_CHECKING:
    # Cloudflare Python Workers provides `fetch` as a runtime global.
    # Declare it for static analyzers (Pylance) only.
    fetch: Any


def _env_text(env, key: str, default: str = "") -> str:
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


async def _watch_call(env, state, method: str, path: str, payload=None):
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    if not calendar_id:
        return None, 400, "missing_google_calendar_id"
    access_token = await get_google_access_token(env, state)
    if not access_token:
        return None, 401, "missing_google_access_token"
    url = (
        "https://www.googleapis.com/calendar/v3/calendars/"
        f"{quote(calendar_id, safe='')}{path}"
    )
    body = None
    if payload is not None:
        payload = dict(payload)
        payload.pop("_state", None)
        body = json.dumps(payload, ensure_ascii=False)
    response = await fetch(
        url,
        {
            "method": method.upper(),
            "headers": {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            "body": body,
        },
    )
    status = int(response.status)
    text = await response.text()
    if status >= 400:
        return None, status, text[:300]
    try:
        data = json.loads(text or "{}")
    except Exception:
        data = {}
    return data, status, ""


async def register_watch(env, state):
    webhook_url = _env_text(env, "GCAL_WEBHOOK_URL", "")
    if not webhook_url:
        return {"ok": False, "error": "missing_gcal_webhook_url"}

    channel_id = _env_text(env, "WATCH_CHANNEL_ID", "") or f"gcal-{uuid4()}"
    payload = {
        "id": channel_id,
        "type": "web_hook",
        "address": webhook_url,
    }
    data, status, error = await _watch_call(env, state, "POST", "/events/watch", payload=payload)
    if data is None:
        return {"ok": False, "status": status, "error": error}

    state_payload = {
        "channel_id": data.get("id"),
        "resource_id": data.get("resourceId"),
        "expiration": data.get("expiration"),
        "calendar_id": _env_text(env, "GOOGLE_CALENDAR_ID", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if state.enabled():
        await state.put_json("gcal_watch_state", state_payload)
    return {"ok": True, "watch_state": state_payload}


async def renew_watch(env, state):
    old_state = await state.get_json("gcal_watch_state", {}) if state.enabled() else {}
    old_channel = str((old_state or {}).get("channel_id") or "")
    old_resource = str((old_state or {}).get("resource_id") or "")

    stop_result = {"ok": True, "skipped": True}
    if old_channel and old_resource:
        access_token = await get_google_access_token(env, state)
        if not access_token:
            return {
                "ok": False,
                "stop": {"ok": False, "error": "missing_google_access_token"},
                "register": {"ok": False, "skipped": True},
            }
        response = await fetch(
            "https://www.googleapis.com/calendar/v3/channels/stop",
            {
                "method": "POST",
                "headers": {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                "body": json.dumps({"id": old_channel, "resourceId": old_resource}),
            },
        )
        stop_result = {"ok": int(response.status) < 400, "status": int(response.status)}

    register_result = await register_watch(env, state)
    return {
        "ok": bool(stop_result.get("ok")) and bool(register_result.get("ok")),
        "stop": stop_result,
        "register": register_result,
    }


def _parse_expiration_epoch_seconds(expiration_value) -> float:
    text = str(expiration_value or "").strip()
    if not text:
        return 0.0
    try:
        # Google watch expiration is usually milliseconds since epoch.
        raw = float(text)
        if raw > 10_000_000_000:
            return raw / 1000.0
        return raw
    except Exception:
        return 0.0


def _renew_threshold_seconds(env) -> float:
    value = _env_text(env, "GCAL_WATCH_RENEW_THRESHOLD_SECONDS", "86400")
    try:
        return max(60.0, float(value))
    except Exception:
        return 86400.0


async def ensure_watch_active(env, state):
    if not state.enabled():
        result = await register_watch(env, state)
        return {"ok": bool(result.get("ok")), "action": "register_no_kv", "result": result}

    current = await state.get_json("gcal_watch_state", {}) or {}
    channel_id = str(current.get("channel_id") or "").strip()
    resource_id = str(current.get("resource_id") or "").strip()
    expires_at = _parse_expiration_epoch_seconds(current.get("expiration"))
    now = datetime.now(timezone.utc).timestamp()
    threshold = _renew_threshold_seconds(env)

    if not channel_id or not resource_id:
        result = await register_watch(env, state)
        return {"ok": bool(result.get("ok")), "action": "register_missing", "result": result}

    if expires_at <= 0:
        result = await renew_watch(env, state)
        return {"ok": bool(result.get("ok")), "action": "renew_no_expiration", "result": result}

    if (expires_at - now) <= threshold:
        result = await renew_watch(env, state)
        return {"ok": bool(result.get("ok")), "action": "renew_expiring", "result": result}

    return {
        "ok": True,
        "action": "noop_valid",
        "watch_state": current,
        "seconds_until_expiration": int(expires_at - now),
    }
