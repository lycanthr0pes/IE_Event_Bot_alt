import json
from urllib.parse import quote
from typing import Any, TYPE_CHECKING

from google_auth import get_google_access_token

try:
    from workers import fetch as _runtime_fetch
except Exception:
    _runtime_fetch = globals().get("fetch")

if _runtime_fetch is None:
    async def fetch(*args, **kwargs):
        raise RuntimeError("fetch_not_available")
else:
    fetch = _runtime_fetch

if TYPE_CHECKING:
    fetch: Any

"""
外部サービス疎通確認モジュール。
- `/admin/migration-status?include_checks=1` から呼ばれ、
  Notion / Discord / Google Calendar の接続状態を返す。
"""


def _env_text(env, key: str, default: str = "") -> str:
    """Worker env から文字列設定を取得する。"""
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


async def check_notion(env):
    """
    Notion API 疎通確認。
    - `/v1/users/me` を呼び、トークン有効性と API 到達性を検証する。
    """
    token = _env_text(env, "NOTION_TOKEN", "")
    if not token:
        return {"ok": False, "status": None, "error": "missing_notion_token"}
    # Notion API ユーザ認証情報リクエスト
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
    # 読み取り
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
    """
    Discord API 疎通確認。
    手順:
    - `/users/@me` を Bot Token で呼び、トークン有効性を確認する。
    """
    token = _env_text(env, "DISCORD_TOKEN", "")
    if not token:
        return {"ok": False, "status": None, "error": "missing_discord_token"}
    # Discord API ユーザ認証情報リクエスト
    response = await fetch(
        "https://discord.com/api/v10/users/@me",
        {
            "method": "GET",
            "headers": {"Authorization": f"Bot {token}"},
        },
    )
    # 読み取り
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
    """
    Google Calendar API 疎通確認。
    手順:
    - `get_google_access_token` でトークンを取得
    - `calendars.get` で対象カレンダーへ到達できるか確認
    """
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    if not calendar_id:
        return {"ok": False, "status": None, "error": "missing_google_calendar_id"}
    access_token = await get_google_access_token(env, state)
    if not access_token:
        return {"ok": False, "status": None, "error": "missing_google_access_token"}
    # Google Calendar API カレンダー情報リクエスト
    response = await fetch(
        "https://www.googleapis.com/calendar/v3/calendars/"
        f"{quote(calendar_id, safe='')}",
        {
            "method": "GET",
            "headers": {"Authorization": f"Bearer {access_token}"},
        },
    )
    # 読み取り
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
    """
    3サービスの疎通確認をまとめて実行し、結果を返す。
    """
    notion = await check_notion(env)
    discord = await check_discord(env)
    google = await check_google_calendar(env, state)
    return {
        "notion": notion,
        "discord": discord,
        "google": google,
    }
