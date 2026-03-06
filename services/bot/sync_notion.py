import json
import os
from datetime import datetime

from config import (
    JST,
    NOTION_EVENT_EXTERNAL_DB_ID,
    NOTION_EVENT_INTERNAL_DB_ID,
    NOTION_QA_DB_ID,
    logger,
    notion_request,
)


CACHE_FILE = "notion_cache.json"
REMINDER_CACHE_FILE = "reminder_cache.json"


async def notion_add_event(
    db_id,
    name,
    content,
    date_iso,
    message_id,
    creator_id,
    event_url=None,
    google_event_id=None,
    location=None,
):
    if not db_id:
        return None
    url = "https://api.notion.com/v1/pages"
    data = {
        "parent": {"database_id": db_id},
        "properties": {
            "イベント名": {"title": [{"text": {"content": name}}]},
            "内容": {"rich_text": [{"text": {"content": content}}]},
            "日時": {"date": {"start": date_iso}},
            "メッセージID": {"rich_text": [{"text": {"content": str(message_id)}}]},
            "作成者ID": {"rich_text": [{"text": {"content": str(creator_id)}}]},
            "ページID": {"rich_text": [{"text": {"content": ""}}]},
        },
    }
    if event_url is not None:
        data["properties"]["イベントURL"] = {"url": event_url}
    if google_event_id is not None:
        data["properties"]["GoogleイベントID"] = {
            "rich_text": [{"text": {"content": str(google_event_id)}}]
        }
    if location is not None:
        data["properties"]["場所"] = {
            "rich_text": [{"text": {"content": str(location)}}]
        }

    status, text, res_data = await notion_request("POST", url, json_body=data)
    if status not in (200, 201):
        logger.error("Notion作成エラー: %s", text)
        return None

    page_id = res_data["id"]
    await notion_update_event(page_id, page_uuid=page_id)
    return page_id


async def notion_get_event(page_id):
    status, _text, data = await notion_request(
        "GET",
        f"https://api.notion.com/v1/pages/{page_id}",
    )
    if status != 200 or not data:
        return None
    return data if "id" in data else None


async def notion_update_event(
    page_id,
    name=None,
    content=None,
    date_iso=None,
    message_id=None,
    page_uuid=None,
    event_url=None,
    google_event_id=None,
    location=None,
):
    props = {}
    if name is not None:
        props["イベント名"] = {"title": [{"text": {"content": name}}]}
    if content is not None:
        props["内容"] = {"rich_text": [{"text": {"content": content}}]}
    if date_iso is not None:
        props["日時"] = {"date": {"start": date_iso}}
    if message_id is not None:
        props["メッセージID"] = {
            "rich_text": [{"text": {"content": str(message_id)}}]
        }
    if page_uuid is not None:
        props["ページID"] = {"rich_text": [{"text": {"content": str(page_uuid)}}]}
    if event_url is not None:
        props["イベントURL"] = {"url": event_url}
    if google_event_id is not None:
        props["GoogleイベントID"] = {
            "rich_text": [{"text": {"content": str(google_event_id)}}]
        }
    if location is not None:
        props["場所"] = {"rich_text": [{"text": {"content": str(location)}}]}

    status, _text, _data = await notion_request(
        "PATCH",
        f"https://api.notion.com/v1/pages/{page_id}",
        json_body={"properties": props},
    )
    return status in (200, 201)


async def notion_delete_event(page_id):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    data = {"archived": True}
    status, text, _res_data = await notion_request("PATCH", url, json_body=data)
    if status not in (200, 201):
        logger.error("Notion削除エラー: %s", text)
        return False
    return True


async def delete_past_events_for_db(db_id):
    if not db_id:
        return
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    status, _text, data = await notion_request("POST", url, json_body={})
    if status != 200 or not data:
        return

    today = datetime.now(JST).date()
    for page in data.get("results", []):
        date_prop = page["properties"]["日時"]["date"]
        if not date_prop:
            continue
        dt = datetime.fromisoformat(date_prop["start"]).date()
        if (today - dt).days >= 30:
            await notion_request(
                "PATCH",
                f"https://api.notion.com/v1/pages/{page['id']}",
                json_body={"archived": True},
            )
            logger.info("[AUTO DELETE] %s をアーカイブ（削除）しました (%s)", page["id"], dt)


async def delete_finished_events_for_db(db_id):
    if not db_id:
        return
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    status, _text, data = await notion_request("POST", url, json_body={})
    if status != 200 or not data:
        return

    now = datetime.now(JST)
    for page in data.get("results", []):
        date_prop = page["properties"]["日時"]["date"]
        if not date_prop:
            continue
        end_iso = date_prop.get("end") or date_prop.get("start")
        if not end_iso:
            continue
        end_dt = datetime.fromisoformat(end_iso)
        if end_dt <= now:
            await notion_request(
                "PATCH",
                f"https://api.notion.com/v1/pages/{page['id']}",
                json_body={"archived": True},
            )
            logger.info("[AUTO DELETE] %s を終了時刻によりアーカイブしました (%s)", page["id"], end_dt)


async def delete_past_events():
    await delete_past_events_for_db(NOTION_EVENT_EXTERNAL_DB_ID)
    await delete_finished_events_for_db(NOTION_EVENT_INTERNAL_DB_ID)


async def fetch_event_pages(db_id):
    if not db_id:
        return []
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    status, text, data = await notion_request("POST", url, json_body={})
    if status != 200:
        logger.error("イベント一覧取得失敗: %s", text)
        return []
    return data.get("results", []) if data else []


async def fetch_qa_db():
    url = f"https://api.notion.com/v1/databases/{NOTION_QA_DB_ID}/query"
    status, _text, data = await notion_request("POST", url, json_body={})
    return data if status == 200 else None


def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(cache, first_run_flag=None):
    if first_run_flag is not None:
        cache["_first_qa_run"] = first_run_flag
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def load_reminder_cache():
    if not os.path.exists(REMINDER_CACHE_FILE):
        return {}
    try:
        with open(REMINDER_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_reminder_cache(cache):
    with open(REMINDER_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


async def get_qa_changes():
    data = await fetch_qa_db()
    if not data:
        return []

    cache = load_cache()
    new_cache = {}
    changes = []
    for page in data.get("results", []):
        pid = page["id"]
        last = page["last_edited_time"]
        new_cache[pid] = last
        if pid not in cache:
            changes.append(("new", page))
        elif cache[pid] != last:
            changes.append(("update", page))

    save_cache(new_cache)
    return changes


def get_question(page) -> str:
    t = page["properties"]["質問"]["title"]
    return t[0]["plain_text"] if t else "(質問なし)"


def get_answer(page) -> str:
    t = page["properties"]["回答"]["rich_text"]
    return t[0]["plain_text"] if t else "(回答なし)"


async def ensure_question_numbers():
    data = await fetch_qa_db()
    if not data:
        return

    pages = data.get("results", [])
    existing_numbers = [
        p["properties"]["質問番号"]["number"]
        for p in pages
        if p["properties"]["質問番号"]["number"] is not None
    ]
    next_num = max(existing_numbers) + 1 if existing_numbers else 1

    missing_pages = [p for p in pages if p["properties"]["質問番号"]["number"] is None]
    missing_pages.sort(key=lambda p: p.get("created_time", ""))

    for page in missing_pages:
        page_id = page["id"]
        url = f"https://api.notion.com/v1/pages/{page_id}"
        data = {"properties": {"質問番号": {"number": next_num}}}
        await notion_request("PATCH", url, json_body=data)
        next_num += 1

    if missing_pages:
        logger.info("新たに %s 件の質問番号を採番しました。", len(missing_pages))
