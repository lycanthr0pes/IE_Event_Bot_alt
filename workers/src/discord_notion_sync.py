import json
from datetime import datetime, timedelta, timezone
from typing import Any, TYPE_CHECKING
from urllib.parse import quote

from google_auth import get_google_access_token

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
Discord Scheduled Events の一覧をポーリングし、前回スナップショットとの差分を
Notion / Google Calendar に反映するモジュール。

設計方針:
- Gateway 依存のリアルタイムイベントではなく、定期実行 + 差分比較で同期する。
- 1回の実行で create/update/delete をまとめて処理する。
- 失敗時は最小限の情報を返し、次回ポーリングで再試行できるようにする。
"""


def _env_text(env, key: str, default: str = "") -> str:
    """
    Worker env から文字列設定を安全に取得する。

    - 未設定時は `default` を返す
    - 文字列の場合は strip() して空文字を default 扱いにする
    """
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _prop(env, key: str, default: str) -> str:
    """
    Notion プロパティ名の解決ヘルパー。
    env 側で上書きされていればそれを使い、なければ既定名を返す。
    """
    return _env_text(env, key, default)


def _notion_headers(env) -> dict:
    """
    Notion REST API 呼び出しに必要な共通ヘッダを返す。
    """
    token = _env_text(env, "NOTION_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def _parse_rfc3339(value: str | None):
    """
    RFC3339 文字列を datetime へ変換する。
    失敗時は None を返し、上位でスキップ判定できるようにする。
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _notion_extract_rich_text(page: dict, prop_name: str):
    """
    Notion ページの rich_text プロパティ先頭要素を文字列として抽出する。
    page: Notion ページオブジェクト
    prop_name: 抽出対象プロパティ名
    返り値:文字列または None
    """
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
    """
    Discord Scheduled Event の開始/終了時刻を datetime として返す。
    - 終了時刻が未設定のイベントがあるため、開始 +1時間で補完する
    - 開始が不正な場合は (None, None) を返して呼び出し側で除外する
    """
    start_dt = _parse_rfc3339((event or {}).get("scheduled_start_time"))
    end_dt = _parse_rfc3339((event or {}).get("scheduled_end_time"))
    if not start_dt:
        return None, None
    if not end_dt or end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=1)
    return start_dt, end_dt


def _date_prop_from_datetimes(start_dt, end_dt):
    """
    datetime を Notion date プロパティ形式へ変換する。
    返り値: {"start": "...", "end": "..."} 形式または None
    """
    if not start_dt:
        return None
    if end_dt and end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=1)
    date_prop = {"start": start_dt.astimezone(timezone.utc).isoformat()}
    if end_dt:
        date_prop["end"] = end_dt.astimezone(timezone.utc).isoformat()
    return date_prop


def _event_location(event: dict):
    """
    Discord event から location(場所) を抽出する。
    entity_metadata.location を優先し、空なら None。
    """
    metadata = (event or {}).get("entity_metadata") or {}
    location = metadata.get("location")
    if not location:
        return None
    text = str(location).strip()
    return text or None


def _normalize_event(event: dict):
    """
    差分検知用に Discord event を正規化する。
    差分判定に不要な項目は落とし、比較対象を安定化する。
    """
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
    """
    正規化イベントを JSON 文字列にして指紋化する。
    前回スナップショットとの文字列比較で更新有無を判定する。
    """
    normalized = _normalize_event(event)
    if not normalized:
        return None
    return json.dumps(normalized, sort_keys=True, ensure_ascii=False)


def _snapshot_status(snapshot_value) -> str:
    """
    保存済みスナップショット文字列から Discord event status を取り出す。
    読み取れない場合は空文字を返す。
    """
    if not snapshot_value:
        return ""
    try:
        data = json.loads(str(snapshot_value))
    except Exception:
        return ""
    return str((data or {}).get("status") or "").strip()


def _should_treat_missing_event_as_delete(snapshot_value) -> bool:
    """
    前回スナップショットにしか存在しないイベントを削除扱いにするか判定する。
    Discord の完了イベントは一覧取得から外れることがあるため、
    completed 相当の status は delete と見なさない。
    """
    status = _snapshot_status(snapshot_value)
    return status not in ("3", "completed", "COMPLETED")


async def _discord_api_request(env, method: str, path: str, payload=None):
    """
    Discord REST API の共通ラッパー。
    返り値: (response_json_or_none, status_code)
    - HTTP 4xx/5xx は None を返す
    - 204(返す本文なし) や空ボディは {} を返す
    """
    token = _env_text(env, "DISCORD_TOKEN", "")
    if not token:
        return None, 401
    url = f"https://discord.com/api/v10{path}" # v10
    body = None if payload is None else json.dumps(payload, ensure_ascii=False)
    # Discord REST API リクエスト
    try:
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
    except Exception as exc:
        detail = str(exc).lower()
        if "too many subrequests" in detail:
            return None, 598
        return None, 599

    # レスポンス読み取り
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
    """
    ギルド(サーバ)のイベント一覧を取得する。
    """
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    if not guild_id:
        return None, "missing_discord_guild_id"
    result, _status = await _discord_api_request(
        env,
        "GET",
        f"/guilds/{guild_id}/scheduled-events?with_user_count=false",
    )
    if not isinstance(result, list):
        status = int(_status or 0)
        if status >= 400:
            return None, f"discord_list_failed:{status}"
        return None, "discord_list_invalid_response"
    return result, None


async def _notion_query_by_message_id(env, db_id: str, message_id: str):
    """
    Notion DB からメッセージID一致のページを1件取得する。
    一致がなければ None。
    """
    if not db_id or not message_id:
        return None
    # \u30e1\u30c3\u30bb\u30fc\u30b8ID = メッセージID
    prop_message_id = _prop(env, "NOTION_PROP_MESSAGE_ID", "\u30e1\u30c3\u30bb\u30fc\u30b8ID")
    # Notion API 用の検索リクエスト本文
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
    # 成功時は200 OK
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
    """
    Notion イベントページを部分更新する。

    設計:
    - 引数が None の項目は更新対象から除外
    - プロパティ名は env の NOTION_PROP_* で上書き可能
    """
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
    message_id: str,
    creator_id: str,
    event_url=None,
    location=None,
    google_event_id=None,
):
    """
    Notion イベントページを新規作成する。
    作成後に page_uuid（ページID）を同ページへ書き戻す。
    """
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

    # Notion API リクエスト
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
    # 読み込み
    if int(response.status) not in (200, 201):
        return None
    data = json.loads(await response.text() or "{}")
    page_id = data.get("id")
    if not page_id:
        return None
    await _notion_update_event(env, page_id, page_uuid=page_id)
    return page_id


async def _notion_archive_page(env, page_id: str):
    """
    Notion ページを archived=true に更新する。
    """
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


def _discord_event_url(env, event_id: str):
    """
    Discordイベントの公開URLを組み立てる。
    """
    guild_id = _env_text(env, "DISCORD_GUILD_ID", "")
    if not guild_id or not event_id:
        return None
    return f"https://discord.com/events/{guild_id}/{event_id}"


def _google_sync_enabled(env) -> bool:
    """
    Discord -> Google 同期を有効化する条件判定。
    必要条件:
    - DISCORD_TO_GOOGLE_SYNC_ENABLED が true
    - GOOGLE_CALENDAR_ID が設定済み
    """
    enabled = str(getattr(env, "DISCORD_TO_GOOGLE_SYNC_ENABLED", "true") or "true").strip().lower()
    if enabled not in ("1", "true", "yes", "on"):
        return False
    return bool(_env_text(env, "GOOGLE_CALENDAR_ID", ""))


def _google_event_body(
    name: str,
    description: str,
    start_dt,
    end_dt,
    location=None,
    discord_event_id: str | None = None,
):
    """
    Discordイベント情報を Google Calendar events API 用のボディに変換する。
    """
    payload = {
        "summary": name,
        "description": description,
        "start": {"dateTime": start_dt.astimezone(timezone.utc).isoformat(), "timeZone": "Asia/Tokyo"},
        "end": {"dateTime": end_dt.astimezone(timezone.utc).isoformat(), "timeZone": "Asia/Tokyo"},
    }
    if location:
        payload["location"] = str(location)
    # Discord 側で作られたイベントであることを示す
    if discord_event_id:
        payload["extendedProperties"] = {
            "private": {
                "ie_origin": "discord",
                "ie_discord_event_id": str(discord_event_id),
            }
        }
    return payload


async def _google_create_event(env, token: str, payload: dict):
    """
    Google Calendar にイベントを新規作成する。
    成功時はレスポンス JSON、失敗時は None。
    """
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    # Google Calendar API リクエスト
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
    """
    Google Calendar イベントを PATCH 更新する。
    """
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    # Google Calendar API リクエスト
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
    """
    Google Calendar イベントを削除する。
    """
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    # Google Calendar API リクエスト
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
    """
    Discordの単一イベントを Notion/Google に同期する。
    処理順:
    1) 時刻/基本情報の正規化
    2) 内部/外部 Notion ページ探索
    3) Google 同期（有効時）: 既存IDがあれば更新、なければ作成
    4) Notion 内部/外部ページへ反映
    """
    # 時刻/基本情報の正規化
    event_id = str((event or {}).get("id") or "")
    if not event_id:
        return True
    name = str((event or {}).get("name") or "(タイトルなし)")
    description = str((event or {}).get("description") or "(本文なし)")
    creator_id = str((event or {}).get("creator_id") or "不明")
    event_url = _discord_event_url(env, event_id)
    location = _event_location(event)
    start_dt, end_dt = _parse_discord_event_times(event)
    if not start_dt:
        return True
    date_prop = _date_prop_from_datetimes(start_dt, end_dt)
    if not date_prop:
        return True
    
    # 内部/外部 Notion ページ探索
    internal_db = _env_text(env, "NOTION_EVENT_INTERNAL_ID", "")
    external_db = _env_text(env, "NOTION_EVENT_ID", "")
    prop_google_id = _prop(env, "NOTION_PROP_GOOGLE_EVENT_ID", "GoogleイベントID")

    internal_page = await _notion_query_by_message_id(env, internal_db, event_id) if internal_db else None
    external_page = await _notion_query_by_message_id(env, external_db, event_id) if external_db else None
    google_event_id = _notion_extract_rich_text(internal_page, prop_google_id) if internal_page else None

    # Google 同期（有効時）: 既存IDがあれば更新、なければ作成
    if _google_sync_enabled(env) and google_token:
        google_payload = _google_event_body(
            name,
            description,
            start_dt,
            end_dt,
            location=location,
            discord_event_id=event_id,
        )
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

    # Notion 内部/外部ページへ反映
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
                google_event_id=google_event_id,
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
                google_event_id=google_event_id,
            )
            if not created_id:
                return False

    return True


async def _sync_discord_event_delete(env, event_id: str, google_token: str | None) -> bool:
    """
    Discord から削除されたイベントを Google/Notion から除去する。
    - Notion 内部ページからGoogleイベントIDを取得できた場合は Google も削除
    - Notion は 内部/外部ページ の双方をアーカイブする。
    """
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
    """
    定期ポーリングのメイン処理。
    手順:
    1) 最大処理件数の制御
    2) Discordイベント一覧を取得して現在のスナップショットを生成
    3) KV の前回のキューとスナップショットと比較して作成/更新/削除を判定
    4) 各差分を Notion / Google に反映
    4) 現在のスナップショットとキューを保存して次回基準にする
    """
    # 現在のDiscord一覧から新スナップショットを生成。
    events, list_error = await _list_discord_scheduled_events(env)
    if list_error:
        return {
            "ok": False,
            "error": list_error,
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "error_count": 1,
            "errors": [list_error],
        }

    current_snapshot = {} # フィンガープリント
    current_events = {} # イベント本体
    for event in events:
        normalized = _normalize_event(event)
        if not normalized:
            continue
        event_id = normalized["id"]
        current_events[event_id] = event
        current_snapshot[event_id] = _fingerprint(event)

    # 前回のスナップショットを取得
    previous_snapshot = await state.get_discord_snapshot() if state.enabled() else {}
    if not isinstance(previous_snapshot, dict):
        previous_snapshot = {}

    had_error = False
    errors = []
    google_token = None
    if _google_sync_enabled(env):
        # 1回の実行内で token を使い回し、外部呼び出し回数を抑える。
        google_token = await get_google_access_token(env, state)

    # スナップショット比較
    created_ids = [eid for eid in current_snapshot.keys() if eid not in previous_snapshot]
    deleted_ids = [
        eid
        for eid in previous_snapshot.keys()
        if eid not in current_snapshot
        and _should_treat_missing_event_as_delete(previous_snapshot.get(eid))
    ]
    updated_ids = [
        eid
        for eid in current_snapshot.keys()
        if eid in previous_snapshot and current_snapshot[eid] != previous_snapshot[eid]
    ]

    # 最大処理件数
    max_changes_raw = _env_text(env, "DISCORD_NOTION_MAX_CHANGES_PER_RUN", "5")
    try:
        max_changes = max(1, int(max_changes_raw))
    except Exception:
        max_changes = 5

    # キュー保存用のキーを決めて、前回の残りを state から読む
    queue_key = "sync:discord_notion_queue"
    queued_ops = []
    if state.enabled():
        raw_queue = await state.get_json(queue_key, [])
        if isinstance(raw_queue, list):
            queued_ops = raw_queue

    # 変更対象IDを重複なくまとめる
    merged_ids = []
    seen_ids = set()
    # まず残りキューから探す
    for op in queued_ops:
        event_id = str((op or {}).get("id") or "").strip()
        if not event_id or event_id in seen_ids:
            continue
        seen_ids.add(event_id)
        merged_ids.append(event_id)
    # 登録された全イベントIDから探す
    for event_id in created_ids + updated_ids + deleted_ids:
        if event_id in seen_ids:
            continue
        seen_ids.add(event_id)
        merged_ids.append(event_id)

    merged_ops = []
    # スナップショットを見て各イベントを作成/更新するか削除するか決める
    for event_id in merged_ids:
        op_type = "upsert" if event_id in current_snapshot else "delete"
        merged_ops.append({"op": op_type, "id": event_id})

    # 変更対象イベントを今回処理する分と残りに分ける
    target_ops = merged_ops[:max_changes]
    remaining_ops = merged_ops[max_changes:]
    processed_count = 0
    retry_ops = []

    # 今回対象の操作を1件ずつ処理
    for op in target_ops:
        op_type = str((op or {}).get("op") or "")
        event_id = str((op or {}).get("id") or "")
        if not event_id:
            continue
        processed_count += 1
        # 作成/更新
        if op_type == "upsert":
            event = current_events.get(event_id)
            if not event:
                retry_ops.append({"op": "delete", "id": event_id})
                continue
            ok = await _sync_discord_event_upsert(env, event, google_token)
            if not ok:
                had_error = True
                errors.append(f"upsert_failed:{event_id}")
                retry_ops.append({"op": "upsert", "id": event_id})
        # 削除
        else:
            ok = await _sync_discord_event_delete(env, event_id, google_token)
            if not ok:
                had_error = True
                errors.append(f"delete_failed:{event_id}")
                retry_ops.append({"op": "delete", "id": event_id})

    pending_changes = len(retry_ops) + len(remaining_ops)

    if state.enabled():
        # 次回差分計算の基準を更新する。
        await state.set_discord_snapshot(current_snapshot)
        await state.put_json_if_changed(queue_key, retry_ops + remaining_ops)

    return {
        "ok": not had_error,
        "created": len(created_ids),
        "updated": len(updated_ids),
        "deleted": len(deleted_ids),
        "processed_changes": processed_count,
        "pending_changes": pending_changes,
        "max_changes_per_run": max_changes,
        "error_count": len(errors),
        "errors": errors[:20],
    }
