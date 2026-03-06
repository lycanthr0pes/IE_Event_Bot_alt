import json
from urllib.parse import quote

from google_auth import get_google_access_token


def _env_text(env, key: str, default: str = "") -> str:
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


async def check_notion(env):
    token = _env_text(env, "NOTION_TOKEN", "")
    if not token:
        return {"ok": False, "status": None, "error": "missing_notion_token"}
    response = await fetch(
        "https://api.notion.com/v1/users/me",
        {
            "method": "GET",
            "headers": {
                "Authorization": f"Bearer {token}",
                "Notion-Version": "2022-06-28",
            },
        },
    )
    status = int(response.status)
    body = await response.text()
    if status >= 400:
        return {"ok": False, "status": status, "error": body[:200]}
    data = {}
    try:
        data = json.loads(body or "{}")
    except Exception:
        data = {}
    return {
        "ok": True,
        "status": status,
        "type": data.get("type"),
    }


async def check_discord(env):
    token = _env_text(env, "DISCORD_TOKEN", "")
    if not token:
        return {"ok": False, "status": None, "error": "missing_discord_token"}
    response = await fetch(
        "https://discord.com/api/v10/users/@me",
        {
            "method": "GET",
            "headers": {"Authorization": f"Bot {token}"},
        },
    )
    status = int(response.status)
    body = await response.text()
    if status >= 400:
        return {"ok": False, "status": status, "error": body[:200]}
    data = {}
    try:
        data = json.loads(body or "{}")
    except Exception:
        data = {}
    return {
        "ok": True,
        "status": status,
        "bot_id": data.get("id"),
        "username": data.get("username"),
    }


async def check_google_calendar(env, state):
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    if not calendar_id:
        return {"ok": False, "status": None, "error": "missing_google_calendar_id"}
    access_token = await get_google_access_token(env, state)
    if not access_token:
        return {"ok": False, "status": None, "error": "missing_google_access_token"}
    response = await fetch(
        "https://www.googleapis.com/calendar/v3/calendars/"
        f"{quote(calendar_id, safe='')}",
        {
            "method": "GET",
            "headers": {"Authorization": f"Bearer {access_token}"},
        },
    )
    status = int(response.status)
    body = await response.text()
    if status >= 400:
        return {"ok": False, "status": status, "error": body[:200]}
    data = {}
    try:
        data = json.loads(body or "{}")
    except Exception:
        data = {}
    return {
        "ok": True,
        "status": status,
        "summary": data.get("summary"),
        "timeZone": data.get("timeZone"),
    }


async def run_connectivity_checks(env, state):
    notion = await check_notion(env)
    discord = await check_discord(env)
    google = await check_google_calendar(env, state)
    return {
        "notion": notion,
        "discord": discord,
        "google": google,
    }
