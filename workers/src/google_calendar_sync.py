import json
from datetime import datetime, timedelta, timezone
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
Google Calendar の差分取得を担当するモジュール。
- Google Events API から updatedMin ベースで変更分を取得
- 次回実行用カーソル（sync:updated_min）を算出
- Notion/Discord への反映は行わず、呼び出し元へ items を返す
"""


def _env_text(env, key: str, default: str = "") -> str:
    """
    Worker env から文字列を安全に取得する。
    """
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


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


def _to_rfc3339_z(dt: datetime) -> str:
    # Google API クエリに使いやすい RFC3339(Z) 形式へ正規化。
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


async def _google_events_list(
    calendar_id: str,
    bearer_token: str,
    *,
    updated_min: str | None,
):
    # Google Calendar API の events.list を最後のページまで全部たどって、イベント一覧をまとめて取得する
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
        # Google API リクエスト
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
        # 読み取り
        status = int(response.status) # HTTPステータス
        text = await response.text()
        if status >= 400:
            # 上位で status を見てリカバリできるように返す。
            return None, status, text
        data = {}
        if text:
            try:
                data = json.loads(text)
            except Exception:
                data = {}
        events.extend(data.get("items") or [])

        # 次ページがあるかどうか判定
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return events, 200, ""


async def run_google_delta_fetch(env, state, *, commit_cursor: bool = True):
    """
    Googleカレンダーの差分イベントを取る
    KV の同期カーソル(updated_min)を更新する
    ただし Notion/Discord への同期はまだやらない
    """
    # 認証情報と対象カレンダーを先に検証し、失敗理由を明示的に返す。
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    bearer_token = await get_google_access_token(env, state)
    if not calendar_id:
        return {"ok": False, "error": "missing_google_calendar_id", "events": 0}
    if not bearer_token:
        return {"ok": False, "error": "missing_google_access_token", "events": 0}

    updated_min = await state.get_sync_updated_min() if state.enabled() else None
    if not updated_min:
        # 初回は 30 日分を取得。
        updated_min = _to_rfc3339_z(datetime.now(timezone.utc) - timedelta(days=30))
    else:
        dt = _parse_rfc3339(updated_min)
        if dt is not None:
            # API 遅延や境界ズレを吸収するため 2 分巻き戻してカーソルを再取得する。
            updated_min = _to_rfc3339_z(dt - timedelta(minutes=2))

    events, status, body = await _google_events_list(
        calendar_id,
        bearer_token,
        updated_min=updated_min,
    )
    if events is None and status == 410:
        # カーソルが古すぎる場合は完全同期にフォールバック。
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
    
    # 取得イベントの updated 時刻を解析
    updated_values = [_parse_rfc3339((e or {}).get("updated")) for e in events]
    updated_values = [d for d in updated_values if d is not None]
    # 取得イベント中の最大 updated を次カーソルにする。
    next_cursor = (
        _to_rfc3339_z(max(updated_values))
        if updated_values
        else _to_rfc3339_z(datetime.now(timezone.utc))
    )
    # カーソル保存
    if commit_cursor and state.enabled():
        await state.set_sync_updated_min(next_cursor)

    return {
        "ok": True,
        "events": len(events),
        "next_updated_min": next_cursor,
        "items": events,
    }
