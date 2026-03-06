import json
import logging
import os
from datetime import datetime, timedelta, timezone

import aiohttp


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("bot")


def getenv_clean(name: str, default=None):
    value = os.getenv(name, default)
    if isinstance(value, str):
        value = value.strip()
        return value if value else default
    return value


DISCORD_TOKEN = getenv_clean("DISCORD_TOKEN")
NOTION_TOKEN = getenv_clean("NOTION_TOKEN")

NOTION_QA_DB_ID = getenv_clean("NOTION_QA_ID")
NOTION_EVENT_EXTERNAL_DB_ID = getenv_clean("NOTION_EVENT_ID")
NOTION_EVENT_INTERNAL_DB_ID = getenv_clean("NOTION_EVENT_INTERNAL_ID")

GOOGLE_CALENDAR_ID = getenv_clean("GOOGLE_CALENDAR_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = getenv_clean("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_SERVICE_ACCOUNT_JSON_PATH = getenv_clean("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")

QA_CHANNEL_ID = int(os.getenv("QA_CHANNEL_ID", 0))
REMINDER_CHANNEL_ID = int(os.getenv("REMINDER_CHANNEL_ID", 0))
REMINDER_ROLE_ID = int(os.getenv("REMINDER_ROLE_ID", 0))
REMINDER_WINDOW_MINUTES = int(os.getenv("REMINDER_WINDOW_MINUTES", 15))
ENABLE_REALTIME_SYNC = getenv_clean("ENABLE_REALTIME_SYNC", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30)

JST = timezone(timedelta(hours=9))


async def notion_request(method: str, url: str, json_body=None):
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
        async with session.request(method, url, headers=headers, json=json_body) as res:
            text = await res.text()
            data = None
            if text:
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    data = None
            return res.status, text, data


def format_display_date(date_iso: str) -> str:
    dt = datetime.fromisoformat(date_iso)
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    w = weekdays[dt.weekday()]
    try:
        return dt.strftime(f"%#m月%#d日（{w}） %H:%M")
    except Exception:
        return dt.strftime(f"%-m月%-d日（{w}） %H:%M")


def to_jst_iso(dt: datetime) -> str:
    return dt.astimezone(JST).isoformat()
