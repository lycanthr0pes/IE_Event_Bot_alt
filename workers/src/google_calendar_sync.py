import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from google_auth import get_google_access_token


def _env_text(env, key: str, default: str = "") -> str:
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _parse_rfc3339(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _to_rfc3339_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


async def _google_events_list(
    calendar_id: str,
    bearer_token: str,
    *,
    updated_min: str | None,
):
    events = []
    page_token = None

    while True:
        params = [
            "singleEvents=true",
            "showDeleted=true",
            "maxResults=2500",
        ]
        if updated_min:
            params.append(f"updatedMin={quote(updated_min, safe=':-T+.Z')}")
        if page_token:
            params.append(f"pageToken={quote(page_token, safe='')}")
        url = (
            "https://www.googleapis.com/calendar/v3/calendars/"
            f"{quote(calendar_id, safe='')}/events?{'&'.join(params)}"
        )
        response = await fetch(
            url,
            {
                "method": "GET",
                "headers": {
                    "Authorization": f"Bearer {bearer_token}",
                    "Accept": "application/json",
                },
            },
        )
        status = int(response.status)
        text = await response.text()
        if status >= 400:
            return None, status, text
        data = {}
        if text:
            try:
                data = json.loads(text)
            except Exception:
                data = {}
        events.extend(data.get("items") or [])
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return events, 200, ""


async def run_google_delta_fetch(env, state, *, commit_cursor: bool = True):
    """
    Fetch Google Calendar delta events and update sync cursor in KV.
    This function does not upsert Notion/Discord yet.
    """
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    bearer_token = await get_google_access_token(env, state)
    if not calendar_id:
        return {"ok": False, "error": "missing_google_calendar_id", "events": 0}
    if not bearer_token:
        return {"ok": False, "error": "missing_google_access_token", "events": 0}

    updated_min = await state.get_sync_updated_min() if state.enabled() else None
    if not updated_min:
        updated_min = _to_rfc3339_z(datetime.now(timezone.utc) - timedelta(days=30))
    else:
        dt = _parse_rfc3339(updated_min)
        if dt is not None:
            updated_min = _to_rfc3339_z(dt - timedelta(minutes=2))

    events, status, body = await _google_events_list(
        calendar_id,
        bearer_token,
        updated_min=updated_min,
    )
    if events is None and status == 410:
        events, status, body = await _google_events_list(
            calendar_id,
            bearer_token,
            updated_min=None,
        )
    if events is None:
        return {
            "ok": False,
            "error": "google_list_failed",
            "status": status,
            "events": 0,
            "body": body[:500],
        }

    updated_values = [_parse_rfc3339((e or {}).get("updated")) for e in events]
    updated_values = [d for d in updated_values if d is not None]
    next_cursor = (
        _to_rfc3339_z(max(updated_values))
        if updated_values
        else _to_rfc3339_z(datetime.now(timezone.utc))
    )
    if commit_cursor and state.enabled():
        await state.set_sync_updated_min(next_cursor)

    return {
        "ok": True,
        "events": len(events),
        "next_updated_min": next_cursor,
        "items": events,
    }
