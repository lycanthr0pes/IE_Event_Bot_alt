import json
from datetime import datetime, timedelta, timezone
from typing import Any, TYPE_CHECKING

try:
    from workers import fetch as _runtime_fetch
except Exception:
    _runtime_fetch = globals().get("fetch")

if _runtime_fetch is None:
    async def fetch(*args, **kwargs):
        raise RuntimeError("fetch_not_available")
else:
    async def fetch(url, options=None):
        opts = options or {}
        try:
            return await _runtime_fetch(
                url,
                method=opts.get("method"),
                headers=opts.get("headers"),
                body=opts.get("body"),
            )
        except TypeError:
            return await _runtime_fetch(url, opts)

if TYPE_CHECKING:
    fetch: Any

"""
Google Calendar 差分イベントを Notion / Discord へ適用するモジュール。

責務:
- GoogleイベントIDを主キーに internal/external Notion DB を upsert
- cancel イベントは Notion アーカイブ + Discord 削除
- gcal<->notion / gcal<->discord の対応マップを KV に保持
"""


def _env_text(env, key: str, default: str = "") -> str:
    """Worker env から文字列設定を取得する。"""
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _env_bool(env, key: str, default: bool) -> bool:
    """Worker env の bool 設定を取得する。"""
    value = getattr(env, key, None)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _prop(env, key: str, default: str) -> str:
    """Notion プロパティ名の env 上書きを解決する。"""
    return _env_text(env, key, default)


def _notion_headers(env) -> dict:
    """Notion API 共通ヘッダを返す。"""
    token = _env_text(env, "NOTION_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def _parse_rfc3339(value: str | None):
    """RFC3339 文字列を datetime へ変換する。失敗時は None。"""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _to_discord_iso(dt):
    """Discord API 向けの UTC ISO8601(Z) へ変換する。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_google_event_times(event: dict):
    """
    Googleイベントの開始/終了時刻を datetime として返す。
    - dateTime と date(日付のみ)の両方を扱う
    - end が欠ける/不正な場合は +1h で補完
    """

    def parse_part(part: dict, is_end: bool = False):
        date_time = part.get("dateTime")
        date_only = part.get("date")
        # 変換
        if date_time:
            dt = _parse_rfc3339(date_time)
            if dt:
                return dt
        if date_only:
            try:
                d = datetime.strptime(date_only, "%Y-%m-%d") # datetime変換
                base = d.replace(tzinfo=timezone(timedelta(hours=9))) # JST(UTC+9)を付ける
                """
                開始なら9時間足す -> 09:00 JST
                終了なら1時間足す -> 01:00 JST
                """
                return base + (timedelta(hours=1) if is_end else timedelta(hours=9))
            except Exception:
                return None
        return None

    start_dt = parse_part((event or {}).get("start") or {}, is_end=False)
    end_dt = parse_part((event or {}).get("end") or {}, is_end=True)
    if not start_dt:
        return None, None
    # 終了が開始以下なら1時間イベントとして補完
    if not end_dt or end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=1)
    return start_dt, end_dt


def _build_notion_date(event: dict):
    """Google の start/end を Notion date 形式へ変換する。"""
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
    """Notion rich_text の先頭要素を文字列化して返す。"""
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


def _resolve_discord_event_id_for_google_event(
    env,
    google_event_id: str,
    notion_page: dict | None,
    fallback_page: dict | None,
    gcal_discord_map: dict,
) -> str | None:
    """
    Googleイベントに対応する Discord event id を既知情報から推定する。

    探索順:
    1) internal/external Notion page の message_id
    2) KV の gcal_discord_map
    """
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "メッセージID")
    notion_discord_id = _notion_extract_rich_text(notion_page, prop_message_id)
    if notion_discord_id:
        return notion_discord_id
    fallback_discord_id = _notion_extract_rich_text(fallback_page, prop_message_id)
    if fallback_discord_id:
        return fallback_discord_id
    mapped_id = str((gcal_discord_map or {}).get(google_event_id) or "").strip()
    return mapped_id or None


def _google_private_props(event: dict) -> dict:
    """Google event.extendedProperties.private を辞書で返す。"""
    props = (((event or {}).get("extendedProperties") or {}).get("private") or {})
    return props if isinstance(props, dict) else {}


def _google_origin_discord_event_id(event: dict) -> str | None:
    """
    Discord 由来で Google に作られたイベントなら元の Discord event id を返す。
    """
    props = _google_private_props(event)
    origin = str(props.get("ie_origin") or "").strip().lower()
    discord_event_id = str(props.get("ie_discord_event_id") or "").strip()
    if origin == "discord" and discord_event_id:
        return discord_event_id
    return None


async def _notion_query_by_google_event_id(env, db_id: str, google_event_id: str):
    """GoogleイベントID一致で Notion ページを1件検索する。"""
    if not db_id or not google_event_id:
        return None
    prop_google_id = _prop(env, "NOTION_PROP_GOOGLE_EVENT_ID", "GoogleイベントID")
    body = {
        "filter": {
            "property": prop_google_id,
            "rich_text": {"equals": str(google_event_id)},
        }
    }
    # Notion API リクエスト
    response = await fetch(
        f"https://api.notion.com/v1/databases/{db_id}/query",
        {
            "method": "POST",
            "headers": _notion_headers(env),
            "body": json.dumps(body, ensure_ascii=False),
        },
    )
    # 読み取り
    if int(response.status) != 200:
        return None
    data = json.loads(await response.text() or "{}")
    results = data.get("results") or []
    return results[0] if results else None


async def _notion_query_by_message_id(env, db_id: str, message_id: str):
    """message_id一致で Notion ページを1件検索する（外部DB互換用途）。"""
    if not db_id or not message_id:
        return None
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "メッセージID")
    body = {
        "filter": {
            "property": prop_message_id,
            "rich_text": {"equals": str(message_id)},
        }
    }
    # Notion API リクエスト
    response = await fetch(
        f"https://api.notion.com/v1/databases/{db_id}/query",
        {
            "method": "POST",
            "headers": _notion_headers(env),
            "body": json.dumps(body, ensure_ascii=False),
        },
    )
    # 読み取り
    if int(response.status) != 200:
        return None
    data = json.loads(await response.text() or "{}")
    results = data.get("results") or []
    return results[0] if results else None


async def _notion_get_page(env, page_id: str):
    """Notion page_id からページを直接取得する。"""
    if not page_id:
        return None
    # Notion API リクエスト
    response = await fetch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {"method": "GET", "headers": _notion_headers(env)},
    )
    # 読み取り
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
    """
    Notion イベントページを部分更新する。

    設計:
    - None の引数は更新対象から除外
    - プロパティ名は NOTION_PROP_* で上書き可能
    """
    if not page_id:
        return False
    prop_title = _prop(env, "NOTION_PROP_TITLE", "イベント名")
    prop_content = _prop(env, "NOTION_PROP_CONTENT", "内容")
    prop_date = _prop(env, "NOTION_PROP_DATE", "日時")
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "メッセージID")
    prop_page_id = _prop(env, "NOTION_PROP_PAGE_ID", "ページID")
    prop_event_url = _prop(env, "NOTION_PROP_EVENT_URL", "イベントURL")
    prop_google_id = _prop(env, "NOTION_PROP_GOOGLE_EVENT_ID", "GoogleイベントID")
    prop_location = _prop(env, "NOTION_PROP_LOCATION", "場所")

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

    # Notion API リクエスト
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
    """
    Notion イベントページを新規作成する。
    作成後に page_uuid を自ページIDで更新する。
    """
    if not db_id:
        return None
    prop_title = _prop(env, "NOTION_PROP_TITLE", "イベント名")
    prop_content = _prop(env, "NOTION_PROP_CONTENT", "内容")
    prop_date = _prop(env, "NOTION_PROP_DATE", "日時")
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "メッセージID")
    prop_creator_id = _prop(env, "NOTION_PROP_CREATOR_ID", "作成者ID")
    prop_page_id = _prop(env, "NOTION_PROP_PAGE_ID", "ページID")
    prop_event_url = _prop(env, "NOTION_PROP_EVENT_URL", "イベントURL")
    prop_google_id = _prop(env, "NOTION_PROP_GOOGLE_EVENT_ID", "GoogleイベントID")
    prop_location = _prop(env, "NOTION_PROP_LOCATION", "場所")

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

    # Notion API リクエスト
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
    # 読み取り
    if int(response.status) not in (200, 201):
        return None
    data = json.loads(await response.text() or "{}")
    page_id = data.get("id")
    if not page_id:
        return None
    await _notion_update_event(env, page_id, page_uuid=page_id)
    return page_id


async def _notion_archive_page(env, page: dict):
    """Notion ページを archived=true に更新(削除)する。"""
    page_id = (page or {}).get("id")
    if not page_id:
        return False
    # Notion API リクエスト
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
    """Discord REST API 共通ラッパー。失敗時は None。"""
    token = _env_text(env, "DISCORD_TOKEN", "")
    if not token:
        return None
    # Dicord REST API リクエスト
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
    # 読み取り
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
    """Discord 反映に必要な設定が揃っているか判定する。"""
    if not _env_bool(env, "DISCORD_SYNC_ENABLED", True):
        return False
    if not _env_text(env, "DISCORD_TOKEN", ""):
        return False
    if not _env_text(env, "DISCORD_GUILD_ID", ""):
        return False
    return True


def _build_discord_description(env, description: str | None, google_event_id: str):
    """
    Discord説明文を組み立てる。
    - 必要なら Googleカレンダー用の識別マーカーを末尾に付ける
    - 長すぎる場合は文字数制限で切る
    """
    base = str(description or "").strip()
    append_marker = _env_bool(env, "DISCORD_APPEND_GCAL_MARKER", False)
    marker_prefix = _env_text(env, "DISCORD_ORIGIN_MARKER_PREFIX", "[gcal-id:")
    limit_raw = _env_text(env, "DISCORD_DESCRIPTION_LIMIT", "1000") # 最大文字数
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
    """Google イベントを Discord イベント payload へ変換する。"""
    google_event_id = str((event or {}).get("id") or "")
    if not google_event_id:
        return None
    start_dt, end_dt = _parse_google_event_times(event)
    if not start_dt:
        return None
    limit_name = int(_env_text(env, "DISCORD_NAME_LIMIT", "100") or "100")
    limit_loc = int(_env_text(env, "DISCORD_LOCATION_LIMIT", "100") or "100")
    # Googleイベントに location が無い場合に使う代替文字列
    fallback_loc = _env_text(env, "DISCORD_LOCATION_FALLBACK", "Google Calendar")
    location = str((event or {}).get("location") or fallback_loc).strip()
    return {
        "name": str((event or {}).get("summary") or "(タイトルなし)")[: max(1, limit_name)],
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
    """Discord イベントを新規作成する。"""
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    payload = _build_discord_payload(env, event)
    if not guild_id or not payload:
        return None
    # Dicord REST API リクエスト
    return await _discord_api_request(
        env,
        "POST",
        f"/guilds/{guild_id}/scheduled-events",
        payload=payload,
    )


async def _discord_update_event(env, discord_event_id: str, event: dict):
    """Discord イベントを更新する。"""
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    payload = _build_discord_payload(env, event)
    if not guild_id or not discord_event_id or not payload:
        return None
    # Dicord REST API リクエスト
    return await _discord_api_request(
        env,
        "PATCH",
        f"/guilds/{guild_id}/scheduled-events/{discord_event_id}",
        payload=payload,
    )


async def _discord_delete_event(env, discord_event_id: str):
    """Discord イベントを削除する。"""
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    if not guild_id or not discord_event_id:
        return False
    # Dicord REST API リクエスト
    result = await _discord_api_request(
        env,
        "DELETE",
        f"/guilds/{guild_id}/scheduled-events/{discord_event_id}",
    )
    return result is not None


async def _sync_to_discord(env, event: dict, notion_page: dict, fallback_page: dict, gcal_discord_map: dict):
    """
    Googleイベントを Discord 側へ同期し、DiscordイベントIDを返す。
ws    Discord ID(message_id / mapped_id) 探索順:
    1) notion_page.message_id
    2) fallback_page.message_id
    3) KVマップ(gcal_discord_map)
    """
    if not _discord_sync_available(env):
        return None
    google_event_id = str((event or {}).get("id") or "")
    if not google_event_id:
        return None
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "メッセージID")
    # notion_page から message_id を読む
    notion_discord_id = _notion_extract_rich_text(notion_page, prop_message_id)
    # もし取れなければ fallback_page から読む
    if not notion_discord_id:
        notion_discord_id = _notion_extract_rich_text(fallback_page, prop_message_id)
    # Google イベントIDをキーにして、対応する Discord イベントID を gcal_discord_map から取得
    mapped_id = str(gcal_discord_map.get(google_event_id) or "")
    # 無ければ None
    discord_event_id = notion_discord_id or mapped_id or None

    # 同期時にGoogle側で削除されていたらDiscord側も削除
    if (event or {}).get("status") == "cancelled":
        if discord_event_id:
            await _discord_delete_event(env, discord_event_id)
        gcal_discord_map.pop(google_event_id, None)
        return None
    
    # discord_event_idがNoneでなければDiscord側更新
    if discord_event_id:
        updated = await _discord_update_event(env, discord_event_id, event)
        if updated is not None:
            resolved = str((updated or {}).get("id") or discord_event_id)
            gcal_discord_map[google_event_id] = resolved
            return resolved
        return None
    # discord_event_idがNoneならDiscord側新規作成
    created = await _discord_create_event(env, event)
    if created and (created or {}).get("id"):
        resolved = str(created["id"])
        gcal_discord_map[google_event_id] = resolved
        return resolved
    return None


async def apply_google_events(env, state, events: list[dict]):
    """
    Google Calendar のイベント一覧を受け取り、Notion と Discord に反映する。
    1回で処理しすぎないように件数制限し、失敗分は次回へ繰り越す。
    """
    internal_db = _env_text(env, "NOTION_EVENT_INTERNAL_ID", "")
    external_db = _env_text(env, "NOTION_EVENT_ID", "")
    if not _env_text(env, "NOTION_TOKEN", "") or not internal_db:
        return {"ok": False, "error": "missing_notion_env", "processed": 0}

    # 1回あたりの最大処理件数を決める
    queue_key = "sync:google_apply_queue"
    max_events_raw = _env_text(env, "GOOGLE_APPLY_MAX_EVENTS_PER_RUN", "10")
    try:
        max_events = max(1, int(max_events_raw))
    except Exception:
        max_events = 10

    # state からマップとキューを読む
    if state.enabled():
        gcal_discord_map = await state.get_gcal_discord_map()
        gcal_notion_map = await state.get_gcal_notion_map()
        queued_events = await state.get_json(queue_key, [])
        if not isinstance(queued_events, list):
            queued_events = []
    else:
        gcal_discord_map = {}
        gcal_notion_map = {"internal": {}, "external": {}}
        queued_events = []
 
    # 今回渡されたイベントと、前回残りをマージ
    # 新イベント
    incoming_events = list(events or [])
    # キュー内イベント
    known_ids = {str((e or {}).get("id") or "") for e in queued_events} 
    # 処理するイベント
    merged_queue = list(queued_events)
    for event in incoming_events:
        event_id = str((event or {}).get("id") or "")
        if not event_id or event_id in known_ids:
            continue
        merged_queue.append(event)
        known_ids.add(event_id)

    # 今回処理する分と、次回に回す分を分ける
    target_events = merged_queue[:max_events]
    remaining_events = merged_queue[max_events:]

    processed = 0
    had_error = False
    errors = []
    internal_map = gcal_notion_map.get("internal") or {}
    external_map = gcal_notion_map.get("external") or {}
    retry_events = []

    # 各Googleイベントを1件ずつ処理
    for event in target_events:
        google_event_id = str((event or {}).get("id") or "")
        if not google_event_id:
            continue
        origin_discord_event_id = _google_origin_discord_event_id(event)
        processed += 1
        event_failed = False
        try:
            # Notion内部DB (マップ -> direct fetch -> DBクエリ).
            page = None
            mapped_internal_page_id = str(internal_map.get(google_event_id) or "")
            # マップで引く
            if mapped_internal_page_id:
                page = await _notion_get_page(env, mapped_internal_page_id)
                # ページが無ければ削除
                if not page:
                    internal_map.pop(google_event_id, None)
            # GoogleイベントIDでDB検索
            if not page:
                page = await _notion_query_by_google_event_id(env, internal_db, google_event_id)
                if page and page.get("id"):
                    internal_map[google_event_id] = str(page["id"])
            # Discord由来なら DiscordイベントID でも探す
            if not page and origin_discord_event_id:
                page = await _notion_query_by_message_id(env, internal_db, origin_discord_event_id)
                if page and page.get("id"):
                    internal_map[google_event_id] = str(page["id"])

            # Notion外部DB (マップ -> direct fetch -> DBクエリ).
            external_page = None
            if external_db:
                mapped_external_page_id = str(external_map.get(google_event_id) or "")
                # マップで引く
                if mapped_external_page_id:
                    external_page = await _notion_get_page(env, mapped_external_page_id)
                    # ページが無ければ削除
                    if not external_page:
                        external_map.pop(google_event_id, None)
                # GoogleイベントIDでDB検索
                if not external_page:
                    external_page = await _notion_query_by_message_id(env, external_db, google_event_id)
                    if not external_page:
                        external_page = await _notion_query_by_google_event_id(env, external_db, google_event_id)
                    if not external_page:
                        discord_event_id = _resolve_discord_event_id_for_google_event(
                            env,
                            google_event_id,
                            page,
                            None,
                            gcal_discord_map,
                        )
                        # Discord同期済みなら、その DiscordイベントID で探す
                        if discord_event_id:
                            external_page = await _notion_query_by_message_id(
                                env,
                                external_db,
                                discord_event_id,
                            )
                    # Discord由来なら DiscordイベントID でも探す
                    if not external_page and origin_discord_event_id:
                        external_page = await _notion_query_by_message_id(
                            env,
                            external_db,
                            origin_discord_event_id,
                        )
                    if external_page and external_page.get("id"):
                        external_map[google_event_id] = str(external_page["id"])

            # キャンセル済みイベントの処理
            if (event or {}).get("status") == "cancelled":
                if page:
                    await _notion_archive_page(env, page)
                    internal_map.pop(google_event_id, None)
                if external_page:
                    await _notion_archive_page(env, external_page)
                    external_map.pop(google_event_id, None)
                if origin_discord_event_id:
                    gcal_discord_map[google_event_id] = origin_discord_event_id
                else:
                    await _sync_to_discord(env, event, page, external_page, gcal_discord_map)
                continue

            # イベント内容を取り出す
            name = str((event or {}).get("summary") or "(untitled)")
            content = str((event or {}).get("description") or "")
            event_url = (event or {}).get("htmlLink")
            location = (event or {}).get("location")
            creator_id = str((((event or {}).get("creator") or {}).get("email")) or "unknown")
            _start_dt, end_dt = _parse_google_event_times(event)
            now_utc = datetime.now(timezone.utc)
            # すでに終わったイベントは内部ページ新規作成しない
            skip_internal_create = page is None and end_dt is not None and end_dt.astimezone(timezone.utc) <= now_utc
            # Notionの日付プロパティを作る
            date_prop = _build_notion_date(event)
            if not date_prop:
                continue

            # 内部Notionページを更新または作成
            # 既存ページがある場合は更新
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
                    event_failed = True
                    errors.append(f"notion_internal_update_failed:{google_event_id}")
                else:
                    internal_map[google_event_id] = str(page["id"])
            # ページが無い場合は作成
            elif not skip_internal_create and not origin_discord_event_id:
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
                    event_failed = True
                    errors.append(f"notion_internal_create_failed:{google_event_id}")
                    continue
                page = {"id": page_id, "properties": {}}
                internal_map[google_event_id] = str(page_id)

            # 外部Notionページを更新または作成
            if external_db:
                # 既存ページがある場合は更新
                if external_page:
                    ext_ok = await _notion_update_event(
                        env,
                        external_page["id"],
                        name=name,
                        content=content,
                        date_prop=date_prop,
                        message_id=google_event_id,
                        google_event_id=google_event_id,
                    )
                    if not ext_ok:
                        had_error = True
                        event_failed = True
                        errors.append(f"notion_external_update_failed:{google_event_id}")
                    else:
                        external_map[google_event_id] = str(external_page["id"])
                # ページが無い場合は作成
                elif not origin_discord_event_id:
                    ext_page_id = await _notion_create_event(
                        env,
                        external_db,
                        name=name,
                        content=content,
                        date_prop=date_prop,
                        creator_id=creator_id,
                        event_url=None,
                        google_event_id=google_event_id,
                        location=None,
                        message_id=google_event_id,
                    )
                    if not ext_page_id:
                        had_error = True
                        event_failed = True
                        errors.append(f"notion_external_create_failed:{google_event_id}")
                    else:
                        external_page = {"id": ext_page_id, "properties": {}}
                        external_map[google_event_id] = str(ext_page_id)

            # Google -> Discord 同期.
            # 元が Discord 由来なら元 Discord イベントIDをそのまま使う
            if origin_discord_event_id:
                discord_event_id = origin_discord_event_id
                gcal_discord_map[google_event_id] = origin_discord_event_id
            # そうでなければ Googleイベントを Discord 側へ作成または更新
            else:
                discord_event_id = await _sync_to_discord(
                    env,
                    event,
                    page,
                    external_page,
                    gcal_discord_map,
                )

            # Notionページに Discord ID を書き戻す
            if page and discord_event_id:
                await _notion_update_event(env, page["id"], message_id=discord_event_id)
            if external_page and discord_event_id:
                await _notion_update_event(env, external_page["id"], message_id=discord_event_id)
        except Exception as exc:
            had_error = True
            event_failed = True
            detail = str(exc)
            if "too many subrequests" in detail.lower():
                errors.append(f"subrequests_exceeded:{google_event_id}")
                retry_events.append(event)
                break
            errors.append(f"exception:{google_event_id}:{type(exc).__name__}")

        if event_failed:
            retry_events.append(event)

    # 次回のためにマップとキューを保存
    gcal_notion_map["internal"] = internal_map
    gcal_notion_map["external"] = external_map
    next_queue = retry_events + remaining_events
    pending_events = len(next_queue)
    if state.enabled():
        await state.set_gcal_discord_map(gcal_discord_map)
        await state.set_gcal_notion_map(gcal_notion_map)
        await state.put_json_if_changed(queue_key, next_queue)

    return {
        "ok": not had_error,
        "processed": processed,
        "pending_events": pending_events,
        "max_events_per_run": max_events,
        "error_count": len(errors),
        "errors": errors[:20],
    }
