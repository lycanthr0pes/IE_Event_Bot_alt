import json
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build

from config import (
    GOOGLE_CALENDAR_ID,
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SERVICE_ACCOUNT_JSON_PATH,
    logger,
    to_jst_iso,
)


GOOGLE_SCOPES = ["https://www.googleapis.com/auth/calendar"]
_google_service = None


def load_service_account_info():
    json_env = GOOGLE_SERVICE_ACCOUNT_JSON
    if json_env:
        if os.path.exists(json_env):
            try:
                with open(json_env, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                logger.error("GoogleサービスアカウントJSON読み込み失敗(path): %s", exc)
                return None
        try:
            return json.loads(json_env)
        except json.JSONDecodeError:
            logger.error(
                "GOOGLE_SERVICE_ACCOUNT_JSON は有効なJSON文字列でもファイルパスでもありません。"
            )
            return None

    if GOOGLE_SERVICE_ACCOUNT_JSON_PATH:
        if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_JSON_PATH):
            logger.error(
                "GOOGLE_SERVICE_ACCOUNT_JSON_PATH のファイルが存在しません: %s",
                GOOGLE_SERVICE_ACCOUNT_JSON_PATH,
            )
            return None
        try:
            with open(GOOGLE_SERVICE_ACCOUNT_JSON_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            logger.error("GoogleサービスアカウントJSON読み込み失敗(path): %s", exc)
            return None

    logger.warning(
        "Google連携無効: GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_JSON_PATH が未設定です。"
    )
    return None


def get_google_calendar_service():
    global _google_service
    if _google_service is not None:
        return _google_service
    if not GOOGLE_CALENDAR_ID:
        logger.warning("Google連携無効: GOOGLE_CALENDAR_ID が未設定です。")
        return None

    info = load_service_account_info()
    if not info:
        return None

    try:
        creds = service_account.Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
        _google_service = build("calendar", "v3", credentials=creds, cache_discovery=False)
        return _google_service
    except Exception as exc:
        logger.error("Google Calendar service 初期化失敗: %s", exc)
        return None


def validate_google_calendar_connection():
    service = get_google_calendar_service()
    if not service:
        return False
    try:
        service.calendars().get(calendarId=GOOGLE_CALENDAR_ID).execute()
        logger.info("Googleカレンダー接続確認OK: %s", GOOGLE_CALENDAR_ID)
        return True
    except Exception as exc:
        logger.error(
            "Googleカレンダー接続確認失敗。カレンダー共有設定/ID/権限を確認してください: %s",
            exc,
        )
        return False


def google_add_event(name, description, start_dt, end_dt, location=None):
    service = get_google_calendar_service()
    if not service:
        logger.warning("Googleカレンダー登録をスキップ: 連携設定が有効化されていません。")
        return None
    start_iso = to_jst_iso(start_dt)
    end_iso = to_jst_iso(end_dt)
    body = {
        "summary": name,
        "description": description,
        "start": {"dateTime": start_iso, "timeZone": "Asia/Tokyo"},
        "end": {"dateTime": end_iso, "timeZone": "Asia/Tokyo"},
    }
    if location:
        body["location"] = str(location)
    try:
        return (
            service.events()
            .insert(calendarId=GOOGLE_CALENDAR_ID, body=body)
            .execute()
        )
    except Exception as exc:
        logger.error("Googleカレンダー追加失敗: %s", exc)
        return None


def google_update_event(google_event_id, name, description, start_dt, end_dt, location=None):
    service = get_google_calendar_service()
    if not service or not google_event_id:
        return None
    start_iso = to_jst_iso(start_dt)
    end_iso = to_jst_iso(end_dt)
    body = {
        "summary": name,
        "description": description,
        "start": {"dateTime": start_iso, "timeZone": "Asia/Tokyo"},
        "end": {"dateTime": end_iso, "timeZone": "Asia/Tokyo"},
    }
    if location:
        body["location"] = str(location)
    try:
        return (
            service.events()
            .patch(calendarId=GOOGLE_CALENDAR_ID, eventId=google_event_id, body=body)
            .execute()
        )
    except Exception as exc:
        logger.error("Googleカレンダー更新失敗(event_id=%s): %s", google_event_id, exc)
        return None


def google_delete_event(google_event_id):
    service = get_google_calendar_service()
    if not service or not google_event_id:
        return False
    try:
        service.events().delete(
            calendarId=GOOGLE_CALENDAR_ID,
            eventId=google_event_id,
        ).execute()
        return True
    except Exception as exc:
        logger.error("Googleカレンダー削除失敗(event_id=%s): %s", google_event_id, exc)
        return False
