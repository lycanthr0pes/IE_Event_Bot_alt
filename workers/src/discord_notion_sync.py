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


def _prop(env, key: str, default: str) -> str:
    return _env_text(env, key, default)


def _notion_headers(env) -> dict:
    token = _env_text(env, "NOTION_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def _parse_rfc3339(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _notion_extract_rich_text(page: dict, prop_name: str):
    props = (page or {}).get("properties", {}) or {}
    rich = ((props.get(prop_name) or {}).get("rich_text") or [])
    if not rich:
        return None
    node = rich[0] or {}
    plain = node.get("plain_text")
    if plain:
        text = str(plain).strip()
        return text or None
    content = (node.get("text") or {}).get("content")
    if content:
        text = str(content).strip()
        return text or None
    return None


def _parse_discord_event_times(event: dict):
    start_dt = _parse_rfc3339((event or {}).get("scheduled_start_time"))
    end_dt = _parse_rfc3339((event or {}).get("scheduled_end_time"))
    if not start_dt:
        return None, None
    if not end_dt or end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=1)
    return start_dt, end_dt


def _date_prop_from_datetimes(start_dt, end_dt):
    if not start_dt:
        return None
    if end_dt and end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=1)
    date_prop = {"start": start_dt.astimezone(timezone.utc).isoformat()}
    if end_dt:
        date_prop["end"] = end_dt.astimezone(timezone.utc).isoformat()
    return date_prop


def _event_location(event: dict):
    metadata = (event or {}).get("entity_metadata") or {}
    location = metadata.get("location")
    if not location:
        return None
    text = str(location).strip()
    return text or None


def _normalize_event(event: dict):
    event_id = str((event or {}).get("id") or "")
    if not event_id:
        return None
    return {
        "id": event_id,
        "name": str((event or {}).get("name") or ""),
        "description": str((event or {}).get("description") or ""),
        "scheduled_start_time": str((event or {}).get("scheduled_start_time") or ""),
        "scheduled_end_time": str((event or {}).get("scheduled_end_time") or ""),
        "location": str(_event_location(event) or ""),
        "status": str((event or {}).get("status") or ""),
    }


def _fingerprint(event: dict):
    normalized = _normalize_event(event)
    if not normalized:
        return None
    return json.dumps(normalized, sort_keys=True, ensure_ascii=False)


async def _discord_api_request(env, method: str, path: str, payload=None):
    token = _env_text(env, "DISCORD_TOKEN", "")
    if not token:
        return None, 401
    url = f"https://discord.com/api/v10{path}"
    body = None if payload is None else json.dumps(payload, ensure_ascii=False)
    response = await fetch(
        url,
        {
            "method": method.upper(),
            "headers": {
                "Authorization": f"Bot {token}",
                "Content-Type": "application/json",
            },
            "body": body,
        },
    )
    status = int(response.status)
    text = await response.text()
    if status >= 400:
        return None, status
    if status == 204 or not text:
        return {}, status
    try:
        return json.loads(text), status
    except Exception:
        return {}, status


async def _list_discord_scheduled_events(env):
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    if not guild_id:
        return []
    result, _status = await _discord_api_request(
        env,
        "GET",
        f"/guilds/{guild_id}/scheduled-events?with_user_count=false",
    )
    if not isinstance(result, list):
        return []
    return result


async def _notion_query_by_message_id(env, db_id: str, message_id: str):
    if not db_id or not message_id:
        return None
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "\u30e1\u30c3\u30bb\u30fc\u30b8ID")
    body = {
        "filter": {
            "property": prop_message_id,
            "rich_text": {"equals": str(message_id)},
        }
    }
    response = await fetch(
        f"https://api.notion.com/v1/databases/{db_id}/query",
        {
            "method": "POST",
            "headers": _notion_headers(env),
            "body": json.dumps(body, ensure_ascii=False),
        },
    )
    if int(response.status) != 200:
        return None
    data = json.loads(await response.text() or "{}")
    results = data.get("results") or []
    return results[0] if results else None


async def _notion_update_event(
    env,
    page_id: str,
    *,
    name=None,
    content=None,
    date_prop=None,
    message_id=None,
    creator_id=None,
    page_uuid=None,
    event_url=None,
    location=None,
    google_event_id=None,
):
    if not page_id:
        return False
    prop_title = _prop(env, "NOTION_PROP_TITLE", "\u30a4\u30d9\u30f3\u30c8\u540d")
    prop_content = _prop(env, "NOTION_PROP_CONTENT", "\u5185\u5bb9")
    prop_date = _prop(env, "NOTION_PROP_DATE", "\u65e5\u6642")
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "\u30e1\u30c3\u30bb\u30fc\u30b8ID")
    prop_creator_id = _prop(env, "NOTION_PROP_CREATOR_ID", "\u4f5c\u6210\u8005ID")
    prop_page_id = _prop(env, "NOTION_PROP_PAGE_ID", "\u30da\u30fc\u30b8ID")
    prop_event_url = _prop(env, "NOTION_PROP_EVENT_URL", "\u30a4\u30d9\u30f3\u30c8URL")
    prop_location = _prop(env, "NOTION_PROP_LOCATION", "\u5834\u6240")
    prop_google_id = _prop(env, "NOTION_PROP_GOOGLE_EVENT_ID", "GoogleイベントID")

    props = {}
    if name is not None:
        props[prop_title] = {"title": [{"text": {"content": str(name)}}]}
    if content is not None:
        props[prop_content] = {"rich_text": [{"text": {"content": str(content)}}]}
    if date_prop is not None:
        props[prop_date] = {"date": date_prop}
    if message_id is not None:
        props[prop_message_id] = {"rich_text": [{"text": {"content": str(message_id)}}]}
    if creator_id is not None:
        props[prop_creator_id] = {"rich_text": [{"text": {"content": str(creator_id)}}]}
    if page_uuid is not None:
        props[prop_page_id] = {"rich_text": [{"text": {"content": str(page_uuid)}}]}
    if event_url is not None:
        props[prop_event_url] = {"url": str(event_url)}
    if location is not None:
        props[prop_location] = {"rich_text": [{"text": {"content": str(location)}}]}
    if google_event_id is not None:
        props[prop_google_id] = {"rich_text": [{"text": {"content": str(google_event_id)}}]}

    response = await fetch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {
            "method": "PATCH",
            "headers": _notion_headers(env),
            "body": json.dumps({"properties": props}, ensure_ascii=False),
        },
    )
    return int(response.status) in (200, 201)


async def _notion_create_event(
    env,
    db_id: str,
    *,
    name: str,
    content: str,
    date_prop: dict,
    message_id: str,
    creator_id: str,
    event_url=None,
    location=None,
    google_event_id=None,
):
    if not db_id:
        return None
    prop_title = _prop(env, "NOTION_PROP_TITLE", "\u30a4\u30d9\u30f3\u30c8\u540d")
    prop_content = _prop(env, "NOTION_PROP_CONTENT", "\u5185\u5bb9")
    prop_date = _prop(env, "NOTION_PROP_DATE", "\u65e5\u6642")
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "\u30e1\u30c3\u30bb\u30fc\u30b8ID")
    prop_creator_id = _prop(env, "NOTION_PROP_CREATOR_ID", "\u4f5c\u6210\u8005ID")
    prop_page_id = _prop(env, "NOTION_PROP_PAGE_ID", "\u30da\u30fc\u30b8ID")
    prop_event_url = _prop(env, "NOTION_PROP_EVENT_URL", "\u30a4\u30d9\u30f3\u30c8URL")
    prop_location = _prop(env, "NOTION_PROP_LOCATION", "\u5834\u6240")
    prop_google_id = _prop(env, "NOTION_PROP_GOOGLE_EVENT_ID", "GoogleイベントID")

    props = {
        prop_title: {"title": [{"text": {"content": str(name)}}]},
        prop_content: {"rich_text": [{"text": {"content": str(content)}}]},
        prop_date: {"date": date_prop},
        prop_message_id: {"rich_text": [{"text": {"content": str(message_id)}}]},
        prop_creator_id: {"rich_text": [{"text": {"content": str(creator_id)}}]},
        prop_page_id: {"rich_text": [{"text": {"content": ""}}]},
    }
    if event_url is not None:
        props[prop_event_url] = {"url": str(event_url)}
    if location is not None:
        props[prop_location] = {"rich_text": [{"text": {"content": str(location)}}]}
    if google_event_id is not None:
        props[prop_google_id] = {"rich_text": [{"text": {"content": str(google_event_id)}}]}

    response = await fetch(
        "https://api.notion.com/v1/pages",
        {
            "method": "POST",
            "headers": _notion_headers(env),
            "body": json.dumps(
                {
                    "parent": {"database_id": db_id},
                    "properties": props,
                },
                ensure_ascii=False,
            ),
        },
    )
    if int(response.status) not in (200, 201):
        return None
    data = json.loads(await response.text() or "{}")
    page_id = data.get("id")
    if not page_id:
        return None
    await _notion_update_event(env, page_id, page_uuid=page_id)
    return page_id


async def _notion_archive_page(env, page_id: str):
    if not page_id:
        return False
    response = await fetch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {
            "method": "PATCH",
            "headers": _notion_headers(env),
            "body": json.dumps({"archived": True}, ensure_ascii=False),
        },
    )
    return int(response.status) in (200, 201)


def _discord_event_url(env, event_id: str):
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    if not guild_id or not event_id:
        return None
    return f"https://discord.com/events/{guild_id}/{event_id}"


def _google_sync_enabled(env) -> bool:
    enabled = str(getattr(env, "DISCORD_TO_GOOGLE_SYNC_ENABLED", "true") or "true").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        return False
    return bool(_env_text(env, "GOOGLE_CALENDAR_ID", ""))


def _google_event_body(name: str, description: str, start_dt, end_dt, location=None):
    payload = {
        "summary": name,
        "description": description,
        "start": {"dateTime": start_dt.astimezone(timezone.utc).isoformat(), "timeZone": "Asia/Tokyo"},
        "end": {"dateTime": end_dt.astimezone(timezone.utc).isoformat(), "timeZone": "Asia/Tokyo"},
    }
    if location:
        payload["location"] = str(location)
    return payload


async def _google_create_event(env, token: str, payload: dict):
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    response = await fetch(
        f"https://www.googleapis.com/calendar/v3/calendars/{quote(calendar_id, safe='')}/events",
        {
            "method": "POST",
            "headers": {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            "body": json.dumps(payload, ensure_ascii=False),
        },
    )
    if int(response.status) >= 400:
        return None
    return json.loads(await response.text() or "{}")


async def _google_update_event(env, token: str, google_event_id: str, payload: dict):
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    response = await fetch(
        "https://www.googleapis.com/calendar/v3/calendars/"
        f"{quote(calendar_id, safe='')}/events/{quote(google_event_id, safe='')}",
        {
            "method": "PATCH",
            "headers": {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            "body": json.dumps(payload, ensure_ascii=False),
        },
    )
    return int(response.status) < 400


async def _google_delete_event(env, token: str, google_event_id: str):
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    response = await fetch(
        "https://www.googleapis.com/calendar/v3/calendars/"
        f"{quote(calendar_id, safe='')}/events/{quote(google_event_id, safe='')}",
        {
            "method": "DELETE",
            "headers": {"Authorization": f"Bearer {token}"},
        },
    )
    return int(response.status) < 400


async def _sync_discord_event_upsert(env, event: dict, google_token: str | None) -> bool:
    event_id = str((event or {}).get("id") or "")
    if not event_id:
        return True
    name = str((event or {}).get("name") or "(no title)")
    description = str((event or {}).get("description") or "(no content)")
    creator_id = str((event or {}).get("creator_id") or "unknown")
    event_url = _discord_event_url(env, event_id)
    location = _event_location(event)
    start_dt, end_dt = _parse_discord_event_times(event)
    if not start_dt:
        return True
    date_prop = _date_prop_from_datetimes(start_dt, end_dt)
    if not date_prop:
        return True

    internal_db = _env_text(env, "NOTION_EVENT_INTERNAL_ID", "")
    external_db = _env_text(env, "NOTION_EVENT_ID", "")
    prop_google_id = _prop(env, "NOTION_PROP_GOOGLE_EVENT_ID", "GoogleイベントID")

    internal_page = await _notion_query_by_message_id(env, internal_db, event_id) if internal_db else None
    external_page = await _notion_query_by_message_id(env, external_db, event_id) if external_db else None
    google_event_id = _notion_extract_rich_text(internal_page, prop_google_id) if internal_page else None

    if _google_sync_enabled(env) and google_token:
        google_payload = _google_event_body(name, description, start_dt, end_dt, location=location)
        if google_event_id:
            google_ok = await _google_update_event(env, google_token, google_event_id, google_payload)
            if not google_ok:
                return False
        else:
            created_google = await _google_create_event(env, google_token, google_payload)
            new_google_id = str((created_google or {}).get("id") or "")
            if not new_google_id:
                return False
            google_event_id = new_google_id

    if internal_db:
        if internal_page:
            ok = await _notion_update_event(
                env,
                internal_page.get("id"),
                name=name,
                content=description,
                date_prop=date_prop,
                message_id=event_id,
                creator_id=creator_id,
                event_url=event_url,
                location=location,
                google_event_id=google_event_id,
            )
            if not ok:
                return False
        else:
            created_id = await _notion_create_event(
                env,
                internal_db,
                name=name,
                content=description,
                date_prop=date_prop,
                message_id=event_id,
                creator_id=creator_id,
                event_url=event_url,
                location=location,
                google_event_id=google_event_id,
            )
            if not created_id:
                return False

    if external_db:
        if external_page:
            ok = await _notion_update_event(
                env,
                external_page.get("id"),
                name=name,
                content=description,
                date_prop=date_prop,
                message_id=event_id,
            )
            if not ok:
                return False
        else:
            created_id = await _notion_create_event(
                env,
                external_db,
                name=name,
                content=description,
                date_prop=date_prop,
                message_id=event_id,
                creator_id=creator_id,
                event_url=None,
                location=None,
            )
            if not created_id:
                return False

    return True


async def _sync_discord_event_delete(env, event_id: str, google_token: str | None) -> bool:
    internal_db = _env_text(env, "NOTION_EVENT_INTERNAL_ID", "")
    external_db = _env_text(env, "NOTION_EVENT_ID", "")
    prop_google_id = _prop(env, "NOTION_PROP_GOOGLE_EVENT_ID", "GoogleイベントID")

    if internal_db:
        internal_page = await _notion_query_by_message_id(env, internal_db, event_id)
        google_event_id = _notion_extract_rich_text(internal_page, prop_google_id) if internal_page else None
        if google_event_id and _google_sync_enabled(env) and google_token:
            deleted = await _google_delete_event(env, google_token, google_event_id)
            if not deleted:
                return False
        if internal_page and not await _notion_archive_page(env, internal_page.get("id")):
            return False
    if external_db:
        external_page = await _notion_query_by_message_id(env, external_db, event_id)
        if external_page and not await _notion_archive_page(env, external_page.get("id")):
            return False
    return True


async def run_discord_notion_poll_sync(env, state):
    events = await _list_discord_scheduled_events(env)
    current_snapshot = {}
    current_events = {}
    for event in events:
        normalized = _normalize_event(event)
        if not normalized:
            continue
        event_id = normalized["id"]
        current_events[event_id] = event
        current_snapshot[event_id] = _fingerprint(event)

    previous_snapshot = await state.get_discord_snapshot() if state.enabled() else {}
    if not isinstance(previous_snapshot, dict):
        previous_snapshot = {}
    had_error = False
    errors = []
    google_token = None
    if _google_sync_enabled(env):
        google_token = await get_google_access_token(env, state)

    created_ids = [eid for eid in current_snapshot.keys() if eid not in previous_snapshot]
    deleted_ids = [eid for eid in previous_snapshot.keys() if eid not in current_snapshot]
    updated_ids = [
        eid
        for eid in current_snapshot.keys()
        if eid in previous_snapshot and current_snapshot[eid] != previous_snapshot[eid]
    ]

    for event_id in created_ids + updated_ids:
        event = current_events.get(event_id)
        if not event:
            continue
        ok = await _sync_discord_event_upsert(env, event, google_token)
        if not ok:
            had_error = True
            errors.append(f"upsert_failed:{event_id}")

    for event_id in deleted_ids:
        ok = await _sync_discord_event_delete(env, event_id, google_token)
        if not ok:
            had_error = True
            errors.append(f"delete_failed:{event_id}")

    if state.enabled():
        await state.set_discord_snapshot(current_snapshot)

    return {
        "ok": not had_error,
        "created": len(created_ids),
        "updated": len(updated_ids),
        "deleted": len(deleted_ids),
        "error_count": len(errors),
        "errors": errors[:20],
    }
