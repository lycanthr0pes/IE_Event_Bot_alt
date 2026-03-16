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
定期ジョブ群（QA通知 / 前日リマインド / Notion cleanup）を提供するモジュール。
- 失敗してもジョブ全体を停止しないよう、エラーは集計して返す
- KV キャッシュで重複通知や初回通知スパイクを抑制する
"""


def _env_text(env, key: str, default: str = "") -> str:
    """Worker env から文字列設定を取得する。"""
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _header_json(token: str | None) -> dict:
    """Notion API 用の共通 JSON ヘッダを返す。"""
    return {
        "Authorization": f"Bearer {token or ''}",
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


def _extract_rich_text(page: dict, prop_name: str):
    """Notion page の rich_text プロパティ先頭を文字列化して返す。"""
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


def _extract_title(page: dict, prop_name: str):
    """Notion ページの title プロパティ先頭を文字列化して返す。"""
    props = (page or {}).get("properties", {}) or {}
    nodes = ((props.get(prop_name) or {}).get("title") or [])
    if not nodes:
        return None
    node = nodes[0] or {}
    text = node.get("plain_text") or ((node.get("text") or {}).get("content"))
    if not text:
        return None
    cleaned = str(text).strip()
    return cleaned or None


def _extract_number(page: dict, prop_name: str):
    """Notion ページの number プロパティ値を返す。"""
    props = (page or {}).get("properties", {}) or {}
    return (props.get(prop_name) or {}).get("number")


def _extract_date(page: dict, prop_name: str):
    """Notion ページの date プロパティ(dict)を返す。"""
    props = (page or {}).get("properties", {}) or {}
    return (props.get(prop_name) or {}).get("date")


async def _notion_query_all_pages(env, db_id: str):
    """
    Notion DB 全件取得（ページネーション対応）。
    途中失敗時は取得済み分を返して終了する。
    """
    if not db_id:
        return []
    token = getattr(env, "NOTION_TOKEN", None)
    if not token:
        return []
    headers = _header_json(token)
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    pages = []
    cursor = None # ページネーション用の cursor
    while True:
        body = {}
        if cursor:
            body["start_cursor"] = cursor
        # Notion query APIリクエスト
        response = await fetch(
            url,
            {
                "method": "POST",
                "headers": headers,
                "body": json.dumps(body, ensure_ascii=False),
            },
        )
        # 読み取り
        if int(response.status) != 200:
            break
        data = json.loads(await response.text() or "{}")
        pages.extend(data.get("results") or [])
        if not data.get("has_more"):
            break
        # 次のカーソルを取得
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return pages


async def _notion_patch_page_number(env, page_id: str, number_value: int) -> bool:
    """Q&A ページの `質問番号` を更新する。"""
    token = getattr(env, "NOTION_TOKEN", None)
    if not token:
        return False
    # Notion API ページ更新リクエスト
    response = await fetch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {
            "method": "PATCH",
            "headers": _header_json(token),
            "body": json.dumps(
                {"properties": {"質問番号": {"number": number_value}}},
                ensure_ascii=False,
            ),
        },
    )
    return int(response.status) in (200, 201)


async def _discord_api_request(env, method: str, path: str, payload=None):
    """
    Discord REST API 共通ラッパー。
    返り値: (response_json_or_none, status_code)
    """
    token = str(getattr(env, "DISCORD_TOKEN", "") or "").strip()
    if not token:
        return None, 401
    url = f"https://discord.com/api/v10{path}"
    body = None if payload is None else json.dumps(payload, ensure_ascii=False)
    # Discord REST APIリクエスト
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
    # 読み取り
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


async def _discord_send_message(env, channel_id: str, content: str, allowed_mentions=None) -> bool:
    """Discord チャンネルへメッセージ送信する。"""
    if not channel_id or not content:
        return False
    payload = {"content": content}
    if allowed_mentions is not None:
        payload["allowed_mentions"] = allowed_mentions
    # 
    # Discord REST API メッセージ送信リクエスト
    res, _status = await _discord_api_request(
        env,
        "POST",
        f"/channels/{channel_id}/messages",
        payload=payload,
    )
    return res is not None


async def ensure_qa_question_numbers(env):
    """
    Q&A DB の `質問番号` 欠番を埋める。
    既存最大番号の次から、作成順で連番を付与する。
    次に使う番号 = 今ある最大番号 + 1
    """
    db_id = str(getattr(env, "NOTION_QA_ID", "") or "").strip()
    if not db_id:
        return
    # DB の全ページを取得
    pages = await _notion_query_all_pages(env, db_id)
    existing = [] # すでに質問番号が付いている番号を入れるリスト
    missing = [] # 質問番号が無いページを入れるリスト
    for page in pages:
        n = _extract_number(page, "質問番号")
        if n is None:
            missing.append(page)
            continue
        try:
            existing.append(int(n))
        except Exception:
            pass
    next_num = (max(existing) + 1) if existing else 1
    missing.sort(key=lambda p: str((p or {}).get("created_time") or ""))
    for page in missing:
        page_id = (page or {}).get("id")
        if not page_id:
            continue
        ok = await _notion_patch_page_number(env, page_id, next_num)
        if ok:
            next_num += 1


async def run_qa_notification_job(env, state, return_detail: bool = False):
    """
    QA通知ジョブ本体。
    - 初回実行時は通知せずキャッシュのみ初期化
    - 最終更新時刻が変わったページのみ判定
    - 回答が空の質問だけ Discord 通知
    """
    db_id = str(getattr(env, "NOTION_QA_ID", "") or "").strip()
    channel_id = str(getattr(env, "QA_CHANNEL_ID", "") or "").strip()
    if not db_id or not channel_id:
        if return_detail:
            return {
                "ok": True,
                "skipped": True,
                "reason": "missing_notion_qa_id_or_qa_channel_id",
            }
        return True

    await ensure_qa_question_numbers(env)
    # Q&A DB の全ページを取得
    pages = await _notion_query_all_pages(env, db_id)
    # キャッシュを取得
    cache = await state.get_json("qa_cache", {}) if state.enabled() else {}
    if not isinstance(cache, dict):
        cache = {}
    # 初回実行判定(キャッシュに _first_qa_run が無ければ True)
    first_run = bool(cache.get("_first_qa_run", True))
    new_cache = {"_first_qa_run": False}
    had_error = False
    failed_page_ids = []

    for page in pages:
        page_id = (page or {}).get("id")
        if not page_id:
            continue
        last = str((page or {}).get("last_edited_time") or "")
        new_cache[page_id] = last
        if first_run:
            continue
        if str(cache.get(page_id) or "") == last:
            continue
        question = _extract_title(page, "質問") or "(質問なし)"
        answer = _extract_rich_text(page, "回答") or "(回答なし)"
        # 回答済みなら通知しない
        if answer != "(回答なし)":
            continue
        q_number = _extract_number(page, "質問番号")
        display = q_number if q_number is not None else "?"
        msg = (
            f"❓ 質問番号 #{display} に更新があります\n"
            f"質問: {question}\n"
            f"回答: {answer}"
        )
        # Discordへ送信
        sent = await _discord_send_message(env, channel_id, msg)
        if not sent:
            had_error = True
            failed_page_ids.append(str(page_id))

    # キャッシュ保存
    if state.enabled():
        await state.put_json_if_changed("qa_cache", new_cache)
    if return_detail:
        return {
            "ok": not had_error,
            "first_run": first_run,
            "failed_count": len(failed_page_ids),
            "failed_page_ids": failed_page_ids[:20],
        }
    return not had_error


async def _list_discord_events(env):
    """Discord ギルド(サーバ)のイベント一覧を取得する。"""
    guild_id = str(getattr(env, "DISCORD_GUILD_ID", "") or "").strip()
    if not guild_id:
        return []
    # Discord REST API イベント情報リクエスト
    result, _status = await _discord_api_request(
        env,
        "GET",
        f"/guilds/{guild_id}/scheduled-events?with_user_count=false",
    )
    if not isinstance(result, list):
        return []
    return result


def _discord_event_url(env, event_id: str):
    """Discord event URL を組み立てる。"""
    guild_id = str(getattr(env, "DISCORD_GUILD_ID", "") or "").strip()
    if not guild_id or not event_id:
        return None
    return f"https://discord.com/events/{guild_id}/{event_id}"


async def run_day_before_reminder_job(env, state, return_detail: bool = False):
    """
    前日リマインド。
    今から24時間後から window_minutes の範囲に入るイベントのみ通知し、
    通知済みIDを reminder_cache に保存して重複送信を防ぐ。
    """
    channel_id = str(getattr(env, "REMINDER_CHANNEL_ID", "") or "").strip()
    role_id = str(getattr(env, "REMINDER_ROLE_ID", "") or "").strip()
    if not channel_id or not role_id:
        if return_detail:
            return {
                "ok": True,
                "skipped": True,
                "reason": "missing_reminder_channel_id_or_role_id",
            }
        return True

    # 通知ウィンドウ幅を取得
    window_minutes_raw = str(getattr(env, "REMINDER_WINDOW_MINUTES", "15") or "15")
    try:
        window_minutes = max(1, int(window_minutes_raw))
    except Exception:
        window_minutes = 15 # 24時間後から15分間

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc + timedelta(hours=24) # 今から24時間後
    # そこからさらに window_minutes 分後
    window_end = window_start + timedelta(minutes=window_minutes)

    events = await _list_discord_events(env)
    # 通知済みイベントIDのキャッシュ
    cache = await state.get_json("reminder_cache", {}) if state.enabled() else {}
    if not isinstance(cache, dict):
        cache = {}
    cache_changed = False
    had_error = False
    failed_event_ids = []

    for event in events:
        event_id = str((event or {}).get("id") or "")
        if not event_id:
            continue
        start_dt = _parse_rfc3339((event or {}).get("scheduled_start_time"))
        if not start_dt:
            continue
        # 今からちょうど24時間後から、さらに window_minutes 分の範囲に始まるイベントのみ
        if not (window_start <= start_dt < window_end):
            continue
        # すでに通知済みならスキップ
        if event_id in cache:
            continue

        event_url = _discord_event_url(env, event_id) or ""
        msg = (
            f"<@&{role_id}>\n"
            "🔔 明日開催のイベントがあります\n"
            f"{event_url}"
        )
        # Discord REST API メッセージ送信リクエスト
        sent = await _discord_send_message(
            env,
            channel_id,
            msg,
            allowed_mentions={"parse": ["roles"], "users": [], "everyone": False},
        )
        # 読み取り
        if sent:
            cache[event_id] = now_utc.isoformat()
            cache_changed = True
        else:
            had_error = True
            failed_event_ids.append(event_id)

    if cache_changed and state.enabled():
        await state.put_json_if_changed("reminder_cache", cache)
    if return_detail:
        return {
            "ok": not had_error,
            "failed_count": len(failed_event_ids),
            "failed_event_ids": failed_event_ids[:20],
        }
    return not had_error


def _utc_now():
    """UTC 現在時刻を返す。"""
    return datetime.now(timezone.utc)


async def _notion_archive_page(env, page_id: str) -> bool:
    """Notion ページを archived=true に更新する。"""
    token = getattr(env, "NOTION_TOKEN", None)
    if not token or not page_id:
        return False
    # Notion API ページ更新リクエスト
    response = await fetch(
        f"https://api.notion.com/v1/pages/{page_id}",
        {
            "method": "PATCH",
            "headers": _header_json(token),
            "body": json.dumps({"archived": True}, ensure_ascii=False),
        },
    )
    return int(response.status) in (200, 201)


def _archive_external_due(date_obj, now_utc: datetime) -> bool:
    """
    外部DBのアーカイブ判定。
    start 日付が「今日から30日以上前」なら true。
    """
    if not isinstance(date_obj, dict):
        return False
    start = _parse_rfc3339(date_obj.get("start"))
    if not start:
        return False
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    age = now_utc.date() - start.astimezone(timezone.utc).date()
    return age.days >= 30


def _archive_internal_due(date_obj, now_utc: datetime) -> bool:
    """
    内部DBのアーカイブ判定。
    end（なければ start）が現在時刻以下なら true。
    """
    if not isinstance(date_obj, dict):
        return False
    end = _parse_rfc3339(date_obj.get("end") or date_obj.get("start"))
    if not end:
        return False
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return end.astimezone(timezone.utc) <= now_utc


def _cleanup_interval_seconds(env) -> int:
    """cleanup ジョブ最小実行間隔（秒）を返す。"""
    raw = _env_text(env, "CLEANUP_INTERVAL_SECONDS", "86400")
    try:
        return max(300, int(raw))
    except Exception:
        return 86400


async def run_auto_clean_job(env, state, return_detail: bool = False):
    """
    Notion cleanup ジョブ本体。
    - interval guard を満たさない場合は skip
    - 外部DB/内部DBの条件に従って対象ページをアーカイブ
    - 最終実行時刻を KV に保存
    """
    internal_db = _env_text(env, "NOTION_EVENT_INTERNAL_ID", "")
    external_db = _env_text(env, "NOTION_EVENT_ID", "")
    date_prop = _env_text(env, "NOTION_PROP_DATE", "日時")
    now_utc = _utc_now()

    # 前回実行時刻 cleanup:last_epoch を読んで、まだ十分時間が経っていなければ処理をスキップ
    if state.enabled():
        last = await state.get_text("cleanup:last_epoch")
        try:
            last_epoch = float(last or "0")
        except Exception:
            last_epoch = 0.0
        if (now_utc.timestamp() - last_epoch) < _cleanup_interval_seconds(env):
            detail = {"ok": True, "skipped": True, "reason": "interval_guard"}
            return detail if return_detail else True

    scanned = 0
    archived = 0
    had_error = False

    # 外部DBを掃除
    if external_db:
        pages = await _notion_query_all_pages(env, external_db)
        for page in pages:
            scanned += 1
            if not _archive_external_due(_extract_date(page, date_prop), now_utc):
                continue
            ok = await _notion_archive_page(env, str((page or {}).get("id") or ""))
            if ok:
                archived += 1
            else:
                had_error = True

    # 外部DBを掃除
    if internal_db:
        pages = await _notion_query_all_pages(env, internal_db)
        for page in pages:
            scanned += 1
            if not _archive_internal_due(_extract_date(page, date_prop), now_utc):
                continue
            ok = await _notion_archive_page(env, str((page or {}).get("id") or ""))
            if ok:
                archived += 1
            else:
                had_error = True

    # 最終実行時刻を保存
    if state.enabled():
        await state.put_text("cleanup:last_epoch", str(now_utc.timestamp()))

    if return_detail:
        return {
            "ok": not had_error,
            "scanned": scanned,
            "archived": archived,
        }
    return not had_error
