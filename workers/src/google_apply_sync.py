import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote


def _env_text(env, key: str, default: str = "") -> str:
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _env_bool(env, key: str, default: bool) -> bool:
    value = getattr(env, key, None)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


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


def _to_discord_iso(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_google_event_times(event: dict):
    def parse_part(part: dict, is_end: bool = False):
        date_time = part.get("dateTime")
        date_only = part.get("date")
        if date_time:
            dt = _parse_rfc3339(date_time)
            if dt:
                return dt
        if date_only:
            try:
                d = datetime.strptime(date_only, "%Y-%m-%d")
                base = d.replace(tzinfo=timezone(timedelta(hours=9)))
                return base + (timedelta(hours=1) if is_end else timedelta(hours=9))
            except Exception:
                return None
        return None

    start_dt = parse_part((event or {}).get("start") or {}, is_end=False)
    end_dt = parse_part((event or {}).get("end") or {}, is_end=True)
    if not start_dt:
        return None, None
    if not end_dt or end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=1)
    return start_dt, end_dt


def _build_notion_date(event: dict):
    start = (event or {}).get("start") or {}
    end = (event or {}).get("end") or {}
    start_iso = start.get("dateTime") or start.get("date")
    end_iso = end.get("dateTime") or end.get("date")
    if not start_iso:
        return None
    payload = {"start": start_iso}
    if end_iso:
        payload["end"] = end_iso
    return payload


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


async def _notion_query_by_google_event_id(env, db_id: str, google_event_id: str):
    if not db_id or not google_event_id:
        return None
    prop_google_id = _prop(
        env,
        "NOTION_PROP_GOOGLE_EVENT_ID",
        "Google\u30a4\u30d9\u30f3\u30c8ID",
    )
    body = {
        "filter": {
            "property": prop_google_id,
            "rich_text": {"equals": str(google_event_id)},
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


async def _notion_get_page(env, page_id: str):
    if not page_id:
        return None
    response = await fetch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {"method": "GET", "headers": _notion_headers(env)},
    )
    if int(response.status) != 200:
        return None
    data = json.loads(await response.text() or "{}")
    return data if data.get("id") else None


async def _notion_update_event(
    env,
    page_id: str,
    *,
    name=None,
    content=None,
    date_prop=None,
    event_url=None,
    google_event_id=None,
    page_uuid=None,
    message_id=None,
    location=None,
):
    if not page_id:
        return False
    prop_title = _prop(env, "NOTION_PROP_TITLE", "\u30a4\u30d9\u30f3\u30c8\u540d")
    prop_content = _prop(env, "NOTION_PROP_CONTENT", "\u5185\u5bb9")
    prop_date = _prop(env, "NOTION_PROP_DATE", "\u65e5\u6642")
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "\u30e1\u30c3\u30bb\u30fc\u30b8ID")
    prop_page_id = _prop(env, "NOTION_PROP_PAGE_ID", "\u30da\u30fc\u30b8ID")
    prop_event_url = _prop(env, "NOTION_PROP_EVENT_URL", "\u30a4\u30d9\u30f3\u30c8URL")
    prop_google_id = _prop(
        env,
        "NOTION_PROP_GOOGLE_EVENT_ID",
        "Google\u30a4\u30d9\u30f3\u30c8ID",
    )
    prop_location = _prop(env, "NOTION_PROP_LOCATION", "\u5834\u6240")

    props = {}
    if name is not None:
        props[prop_title] = {"title": [{"text": {"content": str(name)}}]}
    if content is not None:
        props[prop_content] = {"rich_text": [{"text": {"content": str(content)}}]}
    if date_prop is not None:
        props[prop_date] = {"date": date_prop}
    if event_url is not None:
        props[prop_event_url] = {"url": str(event_url)}
    if google_event_id is not None:
        props[prop_google_id] = {"rich_text": [{"text": {"content": str(google_event_id)}}]}
    if page_uuid is not None:
        props[prop_page_id] = {"rich_text": [{"text": {"content": str(page_uuid)}}]}
    if message_id is not None:
        props[prop_message_id] = {"rich_text": [{"text": {"content": str(message_id)}}]}
    if location is not None:
        props[prop_location] = {"rich_text": [{"text": {"content": str(location)}}]}

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
    creator_id: str,
    event_url,
    google_event_id,
    message_id,
    location=None,
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
    prop_google_id = _prop(
        env,
        "NOTION_PROP_GOOGLE_EVENT_ID",
        "Google\u30a4\u30d9\u30f3\u30c8ID",
    )
    prop_location = _prop(env, "NOTION_PROP_LOCATION", "\u5834\u6240")

    props = {
        prop_title: {"title": [{"text": {"content": str(name)}}]},
        prop_content: {"rich_text": [{"text": {"content": str(content)}}]},
        prop_date: {"date": date_prop},
        prop_message_id: {"rich_text": [{"text": {"content": str(message_id or "")}}]},
        prop_creator_id: {"rich_text": [{"text": {"content": str(creator_id)}}]},
        prop_page_id: {"rich_text": [{"text": {"content": ""}}]},
    }
    if event_url is not None:
        props[prop_event_url] = {"url": str(event_url)}
    if google_event_id is not None:
        props[prop_google_id] = {"rich_text": [{"text": {"content": str(google_event_id)}}]}
    if location is not None:
        props[prop_location] = {"rich_text": [{"text": {"content": str(location)}}]}

    response = await fetch(
        "https://api.notion.com/v1/pages",
        {
            "method": "POST",
            "headers": _notion_headers(env),
            "body": json.dumps(
                {"parent": {"database_id": db_id}, "properties": props},
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


async def _notion_archive_page(env, page: dict):
    page_id = (page or {}).get("id")
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


async def _discord_api_request(env, method: str, path: str, payload=None):
    token = _env_text(env, "DISCORD_TOKEN", "")
    if not token:
        return None
    response = await fetch(
        f"https://discord.com/api/v10{path}",
        {
            "method": method.upper(),
            "headers": {
                "Authorization": f"Bot {token}",
                "Content-Type": "application/json",
            },
            "body": None if payload is None else json.dumps(payload, ensure_ascii=False),
        },
    )
    if int(response.status) >= 400:
        return None
    text = await response.text()
    if int(response.status) == 204 or not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        return {}


def _discord_sync_available(env):
    if not _env_bool(env, "DISCORD_SYNC_ENABLED", True):
        return False
    if not _env_text(env, "DISCORD_TOKEN", ""):
        return False
    if not _env_text(env, "DISCORD_GUILD_ID", ""):
        return False
    return True


def _build_discord_description(env, description: str | None, google_event_id: str):
    base = str(description or "").strip()
    append_marker = _env_bool(env, "DISCORD_APPEND_GCAL_MARKER", False)
    marker_prefix = _env_text(env, "DISCORD_ORIGIN_MARKER_PREFIX", "[gcal-id:")
    limit_raw = _env_text(env, "DISCORD_DESCRIPTION_LIMIT", "1000")
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 1000
    if append_marker:
        marker = f"{marker_prefix}{google_event_id}]"
        text = f"{base}\n\n{marker}" if base else marker
    else:
        text = base
    return text[: max(1, limit)]


def _build_discord_payload(env, event: dict):
    google_event_id = str((event or {}).get("id") or "")
    if not google_event_id:
        return None
    start_dt, end_dt = _parse_google_event_times(event)
    if not start_dt:
        return None
    limit_name = int(_env_text(env, "DISCORD_NAME_LIMIT", "100") or "100")
    limit_loc = int(_env_text(env, "DISCORD_LOCATION_LIMIT", "100") or "100")
    fallback_loc = _env_text(env, "DISCORD_LOCATION_FALLBACK", "Google Calendar")
    location = str((event or {}).get("location") or fallback_loc).strip()
    return {
        "name": str((event or {}).get("summary") or "(no title)")[: max(1, limit_name)],
        "description": _build_discord_description(
            env,
            (event or {}).get("description"),
            google_event_id,
        ),
        "privacy_level": 2,
        "entity_type": 3,
        "scheduled_start_time": _to_discord_iso(start_dt),
        "scheduled_end_time": _to_discord_iso(end_dt),
        "entity_metadata": {"location": location[: max(1, limit_loc)]},
    }


async def _discord_create_event(env, event: dict):
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    payload = _build_discord_payload(env, event)
    if not guild_id or not payload:
        return None
    return await _discord_api_request(
        env,
        "POST",
        f"/guilds/{guild_id}/scheduled-events",
        payload=payload,
    )


async def _discord_update_event(env, discord_event_id: str, event: dict):
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    payload = _build_discord_payload(env, event)
    if not guild_id or not discord_event_id or not payload:
        return None
    return await _discord_api_request(
        env,
        "PATCH",
        f"/guilds/{guild_id}/scheduled-events/{discord_event_id}",
        payload=payload,
    )


async def _discord_delete_event(env, discord_event_id: str):
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    if not guild_id or not discord_event_id:
        return False
    result = await _discord_api_request(
        env,
        "DELETE",
        f"/guilds/{guild_id}/scheduled-events/{discord_event_id}",
    )
    return result is not None


async def _sync_to_discord(env, event: dict, notion_page: dict, fallback_page: dict, gcal_discord_map: dict):
    if not _discord_sync_available(env):
        return None
    google_event_id = str((event or {}).get("id") or "")
    if not google_event_id:
        return None
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "\u30e1\u30c3\u30bb\u30fc\u30b8ID")
    notion_discord_id = _notion_extract_rich_text(notion_page, prop_message_id)
    if not notion_discord_id:
        notion_discord_id = _notion_extract_rich_text(fallback_page, prop_message_id)
    mapped_id = str(gcal_discord_map.get(google_event_id) or "")
    discord_event_id = notion_discord_id or mapped_id or None

    if (event or {}).get("status") == "cancelled":
        if discord_event_id:
            await _discord_delete_event(env, discord_event_id)
        gcal_discord_map.pop(google_event_id, None)
        return None

    if discord_event_id:
        updated = await _discord_update_event(env, discord_event_id, event)
        if updated is not None:
            resolved = str((updated or {}).get("id") or discord_event_id)
            gcal_discord_map[google_event_id] = resolved
            return resolved
        return None

    created = await _discord_create_event(env, event)
    if created and (created or {}).get("id"):
        resolved = str(created["id"])
        gcal_discord_map[google_event_id] = resolved
        return resolved
    return None


async def apply_google_events(env, state, events: list[dict]):
    internal_db = _env_text(env, "NOTION_EVENT_INTERNAL_ID", "")
    external_db = _env_text(env, "NOTION_EVENT_ID", "")
    if not _env_text(env, "NOTION_TOKEN", "") or not internal_db:
        return {"ok": False, "error": "missing_notion_env", "processed": 0}

    if state.enabled():
        gcal_discord_map = await state.get_gcal_discord_map()
        gcal_notion_map = await state.get_gcal_notion_map()
    else:
        gcal_discord_map = {}
        gcal_notion_map = {"internal": {}, "external": {}}

    processed = 0
    had_error = False
    errors = []
    internal_map = gcal_notion_map.get("internal") or {}
    external_map = gcal_notion_map.get("external") or {}

    for event in events or []:
        google_event_id = str((event or {}).get("id") or "")
        if not google_event_id:
            continue
        processed += 1
        try:
            page = None
            mapped_internal_page_id = str(internal_map.get(google_event_id) or "")
            if mapped_internal_page_id:
                page = await _notion_get_page(env, mapped_internal_page_id)
                if not page:
                    internal_map.pop(google_event_id, None)
            if not page:
                page = await _notion_query_by_google_event_id(env, internal_db, google_event_id)
                if page and page.get("id"):
                    internal_map[google_event_id] = str(page["id"])

            external_page = None
            if external_db:
                mapped_external_page_id = str(external_map.get(google_event_id) or "")
                if mapped_external_page_id:
                    external_page = await _notion_get_page(env, mapped_external_page_id)
                    if not external_page:
                        external_map.pop(google_event_id, None)
                if not external_page:
                    external_page = await _notion_query_by_message_id(env, external_db, google_event_id)
                    if not external_page:
                        external_page = await _notion_query_by_google_event_id(
                            env,
                            external_db,
                            google_event_id,
                        )
                    if external_page and external_page.get("id"):
                        external_map[google_event_id] = str(external_page["id"])

            if (event or {}).get("status") == "cancelled":
                if page:
                    await _notion_archive_page(env, page)
                    internal_map.pop(google_event_id, None)
                if external_page:
                    await _notion_archive_page(env, external_page)
                    external_map.pop(google_event_id, None)
                await _sync_to_discord(env, event, page, external_page, gcal_discord_map)
                continue

            name = str((event or {}).get("summary") or "(no title)")
            content = str((event or {}).get("description") or "(no content)")
            event_url = (event or {}).get("htmlLink")
            location = (event or {}).get("location")
            creator_id = str((((event or {}).get("creator") or {}).get("email")) or "unknown")
            _start_dt, end_dt = _parse_google_event_times(event)
            now_utc = datetime.now(timezone.utc)
            skip_internal_create = (
                page is None and end_dt is not None and end_dt.astimezone(timezone.utc) <= now_utc
            )
            date_prop = _build_notion_date(event)
            if not date_prop:
                continue

            if page:
                ok = await _notion_update_event(
                    env,
                    page["id"],
                    name=name,
                    content=content,
                    date_prop=date_prop,
                    event_url=event_url,
                    google_event_id=google_event_id,
                    location=location,
                )
                if not ok:
                    had_error = True
                    errors.append(f"notion_internal_update_failed:{google_event_id}")
                else:
                    internal_map[google_event_id] = str(page["id"])
            elif not skip_internal_create:
                page_id = await _notion_create_event(
                    env,
                    internal_db,
                    name=name,
                    content=content,
                    date_prop=date_prop,
                    creator_id=creator_id,
                    event_url=event_url,
                    google_event_id=google_event_id,
                    location=location,
                    message_id="",
                )
                if not page_id:
                    had_error = True
                    errors.append(f"notion_internal_create_failed:{google_event_id}")
                    continue
                page = {"id": page_id, "properties": {}}
                internal_map[google_event_id] = str(page_id)

            if external_db:
                if external_page:
                    ext_ok = await _notion_update_event(
                        env,
                        external_page["id"],
                        name=name,
                        content=content,
                        date_prop=date_prop,
                        message_id=google_event_id,
                    )
                    if not ext_ok:
                        had_error = True
                        errors.append(f"notion_external_update_failed:{google_event_id}")
                    else:
                        external_map[google_event_id] = str(external_page["id"])
                else:
                    ext_page_id = await _notion_create_event(
                        env,
                        external_db,
                        name=name,
                        content=content,
                        date_prop=date_prop,
                        creator_id=creator_id,
                        event_url=None,
                        google_event_id=None,
                        location=None,
                        message_id=google_event_id,
                    )
                    if not ext_page_id:
                        had_error = True
                        errors.append(f"notion_external_create_failed:{google_event_id}")
                    else:
                        external_page = {"id": ext_page_id, "properties": {}}
                        external_map[google_event_id] = str(ext_page_id)

            discord_event_id = await _sync_to_discord(
                env,
                event,
                page,
                external_page,
                gcal_discord_map,
            )
            if page and discord_event_id:
                await _notion_update_event(env, page["id"], message_id=discord_event_id)
            if external_page and discord_event_id:
                await _notion_update_event(env, external_page["id"], message_id=discord_event_id)
        except Exception:
            had_error = True
            errors.append(f"exception:{google_event_id}")

    gcal_notion_map["internal"] = internal_map
    gcal_notion_map["external"] = external_map
    if state.enabled():
        await state.set_gcal_discord_map(gcal_discord_map)
        await state.set_gcal_notion_map(gcal_notion_map)

    return {
        "ok": not had_error,
        "processed": processed,
        "error_count": len(errors),
        "errors": errors[:20],
    }
