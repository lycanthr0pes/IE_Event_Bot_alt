import json
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING
from urllib.parse import quote
from uuid import uuid4

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


def _env_text(env, key: str, default: str = "") -> str:
    """
    Worker env から文字列設定を取得する。

    - 未設定時は default
    - 文字列は trim して空文字を default 扱い
    """
    value = getattr(env, key, None)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


async def _watch_call(env, state, method: str, path: str, payload=None):
    """
    Google Calendar watch 関連 API 呼び出しの共通ラッパー。

    変数:
        method: HTTP メソッド
        path: `/events/watch` など calendar 配下パス
        payload: JSON ボディ
    返り値: (data_or_none, status_code, error_text)
    """
    calendar_id = _env_text(env, "GOOGLE_CALENDAR_ID", "")
    if not calendar_id:
        return None, 400, "missing_google_calendar_id"
    access_token = await get_google_access_token(env, state)
    if not access_token:
        return None, 401, "missing_google_access_token"
    url = (
        "https://www.googleapis.com/calendar/v3/calendars/"
        f"{quote(calendar_id, safe='')}{path}"
    )
    body = None
    if payload is not None:
        payload = dict(payload)
        # _stateを落とす
        payload.pop("_state", None)
        body = json.dumps(payload, ensure_ascii=False)

    # watch API リクエスト
    response = await fetch(
        url,
        {
            "method": method.upper(),
            "headers": {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            "body": body,
        },
    )
    # 読み取り
    status = int(response.status)
    text = await response.text()
    if status >= 400:
        return None, status, text[:300]
    try:
        data = json.loads(text or "{}")
    except Exception:
        data = {}
    return data, status, ""


async def register_watch(env, state):
    """
    Google Calendar events.watch を新規登録する。
    成功時:
    - channel/resource/expiration を KV(`gcal_watch_state`) に保存
    失敗時:
    - `ok: false` と status/error を返す
    """
    webhook_url = _env_text(env, "GCAL_WEBHOOK_URL", "")
    if not webhook_url:
        return {"ok": False, "error": "missing_gcal_webhook_url"}

    channel_id = _env_text(env, "WATCH_CHANNEL_ID", "") or f"gcal-{uuid4()}"
    payload = {
        "id": channel_id,
        "type": "web_hook",
        "address": webhook_url,
    }
    data, status, error = await _watch_call(env, state, "POST", "/events/watch", payload=payload)
    if data is None:
        return {"ok": False, "status": status, "error": error}

    state_payload = {
        "channel_id": data.get("id"),
        "resource_id": data.get("resourceId"),
        "expiration": data.get("expiration"),
        "calendar_id": _env_text(env, "GOOGLE_CALENDAR_ID", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    # watch保存
    if state.enabled():
        await state.put_json_if_changed("gcal_watch_state", state_payload)
    return {"ok": True, "watch_state": state_payload}


async def renew_watch(env, state):
    """
    既存 watch を更新する。
    手順:
    1) 既存 state に channel/resource があれば channels.stop
    2) register_watch で新しい watch を作成
    """
    # 既存の watch 状態を読む
    old_state = await state.get_json("gcal_watch_state", {}) if state.enabled() else {}
    old_channel = str((old_state or {}).get("channel_id") or "")
    old_resource = str((old_state or {}).get("resource_id") or "")
    # デフォルトの stop 結果
    stop_result = {"ok": True, "skipped": True}
    # 古い watch がある場合だけ stop を試す
    if old_channel and old_resource:
        access_token = await get_google_access_token(env, state)
        if not access_token:
            return {
                "ok": False,
                "stop": {"ok": False, "error": "missing_google_access_token"},
                "register": {"ok": False, "skipped": True},
            }
        # channels.stop を呼ぶ(Google API リクエスト)
        response = await fetch(
            "https://www.googleapis.com/calendar/v3/channels/stop",
            {
                "method": "POST",
                "headers": {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                "body": json.dumps({"id": old_channel, "resourceId": old_resource}),
            },
        )
        stop_result = {"ok": int(response.status) < 400, "status": int(response.status)}

    register_result = await register_watch(env, state)
    return {
        "ok": bool(stop_result.get("ok")) and bool(register_result.get("ok")),
        "stop": stop_result,
        "register": register_result,
    }


def _parse_expiration_epoch_seconds(expiration_value) -> float:
    """
    Google watch expiration (ミリ秒)を桁を見て秒に変換する。
    """
    text = str(expiration_value or "").strip()
    if not text:
        return 0.0
    try:
        raw = float(text)
        if raw > 10_000_000_000:
            return raw / 1000.0
        return raw
    except Exception:
        return 0.0


def _renew_threshold_seconds(env) -> float:
    """
    watch 更新しきい値（秒）を返す。
    最低 60 秒を保証する。
    """
    value = _env_text(env, "GCAL_WATCH_RENEW_THRESHOLD_SECONDS", "86400")
    try:
        return max(60.0, float(value))
    except Exception:
        return 86400.0


async def ensure_watch_active(env, state):
    """
    watch が有効な状態を保つ。
    分岐:
    - KV 無効: 登録のみ試行
    - state 不足: register
    - 期限不明: renew
    - 期限がしきい値以下: renew
    - それ以外: noop
    """
    # state が無効(watch 保存比較不可)なら register
    if not state.enabled():
        result = await register_watch(env, state)
        return {"ok": bool(result.get("ok")), "action": "register_no_kv", "result": result}

    current = await state.get_json("gcal_watch_state", {}) or {}
    channel_id = str(current.get("channel_id") or "").strip()
    resource_id = str(current.get("resource_id") or "").strip()
    expires_at = _parse_expiration_epoch_seconds(current.get("expiration"))
    now = datetime.now(timezone.utc).timestamp()
    threshold = _renew_threshold_seconds(env)

    # channel_id / resource_id が無ければ register
    if not channel_id or not resource_id:
        result = await register_watch(env, state)
        return {"ok": bool(result.get("ok")), "action": "register_missing", "result": result}

    # 期限不明なら renew
    if expires_at <= 0:
        result = await renew_watch(env, state)
        return {"ok": bool(result.get("ok")), "action": "renew_no_expiration", "result": result}

    # 期限が近ければ renew
    if (expires_at - now) <= threshold:
        result = await renew_watch(env, state)
        return {"ok": bool(result.get("ok")), "action": "renew_expiring", "result": result}

    # まだ十分有効なら何もしない
    return {
        "ok": True,
        "action": "noop_valid",
        "watch_state": current,
        "seconds_until_expiration": int(expires_at - now),
    }
