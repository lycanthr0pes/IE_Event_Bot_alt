from datetime import timedelta

from config import (
    NOTION_EVENT_EXTERNAL_DB_ID,
    NOTION_EVENT_INTERNAL_DB_ID,
    logger,
    to_jst_iso,
)
from sync_google import google_add_event, google_delete_event, google_update_event
from sync_notion import (
    fetch_event_pages,
    notion_add_event,
    notion_delete_event,
    notion_get_event,
    notion_update_event,
)


def is_ignored_event(name: str) -> bool:
    return "定例会" in name


def get_event_url(event) -> str:
    url = getattr(event, "url", None)
    if url:
        return str(url)
    guild_id = getattr(event, "guild_id", None)
    if guild_id:
        return f"https://discord.com/events/{guild_id}/{event.id}"
    return None


def get_event_location(event) -> str:
    location = getattr(event, "location", None)
    if location:
        text = str(location).strip()
        if text:
            return text
    metadata = getattr(event, "entity_metadata", None)
    meta_location = getattr(metadata, "location", None) if metadata else None
    if meta_location:
        text = str(meta_location).strip()
        if text:
            return text
    return None


def get_google_event_id_from_notion_page(page) -> str:
    if not page:
        return None
    props = page.get("properties", {})
    rich = props.get("GoogleイベントID", {}).get("rich_text", [])
    if not rich:
        return None
    node = rich[0]
    plain = node.get("plain_text")
    if plain:
        return str(plain).strip() or None
    content = node.get("text", {}).get("content")
    if content:
        return str(content).strip() or None
    return None


def is_bot_created_scheduled_event(bot, event) -> bool:
    user = getattr(bot, "user", None)
    if not user:
        return False
    creator_id = getattr(event, "creator_id", None)
    if creator_id is not None and int(creator_id) == int(user.id):
        return True
    creator = getattr(event, "creator", None)
    creator_obj_id = getattr(creator, "id", None) if creator else None
    if creator_obj_id is not None and int(creator_obj_id) == int(user.id):
        return True
    return False


async def find_event_page(db_id, event_id_str):
    pages = await fetch_event_pages(db_id)
    for page in pages:
        prop = page["properties"].get("メッセージID", {}).get("rich_text", [])
        if not prop:
            continue
        mid = prop[0]["text"]["content"]
        if mid == event_id_str:
            return page
    return None


async def handle_scheduled_event_create(bot, event):
    name = event.name
    if is_bot_created_scheduled_event(bot, event):
        logger.info("Bot作成イベントのためDiscord->Google/Notion同期をスキップ: %s", name)
        return
    description = event.description or "(内容なし)"
    start_iso = to_jst_iso(event.start_time)
    event_url = get_event_url(event)
    event_location = get_event_location(event)
    creator_id = event.creator_id or (event.creator.id if event.creator else "unknown")

    end_time = event.end_time or (event.start_time + timedelta(hours=1))
    google_event = google_add_event(
        name,
        description,
        event.start_time,
        end_time,
        location=event_location,
    )
    google_event_id = google_event.get("id") if google_event else None

    if not is_ignored_event(event.name):
        await notion_add_event(
            NOTION_EVENT_EXTERNAL_DB_ID,
            name=name,
            content=description,
            date_iso=start_iso,
            message_id=event.id,
            creator_id=creator_id,
            google_event_id=google_event_id,
        )
    else:
        logger.warning("外部用DBは除外イベントのため登録しません: %s", event.name)

    await notion_add_event(
        NOTION_EVENT_INTERNAL_DB_ID,
        name=name,
        content=description,
        date_iso=start_iso,
        message_id=event.id,
        creator_id=creator_id,
        event_url=event_url,
        google_event_id=google_event_id,
        location=event_location,
    )
    logger.info("Discordイベント作成 -> Notion登録: %s", name)


async def handle_scheduled_event_update(bot, before, after):
    _ = before
    after_id_str = str(after.id)
    if is_bot_created_scheduled_event(bot, after):
        logger.info("Bot作成イベントのためDiscord更新同期をスキップ: %s", after.name)
        return
    event_url = get_event_url(after)

    target = None
    if not is_ignored_event(after.name):
        target = await find_event_page(NOTION_EVENT_EXTERNAL_DB_ID, after_id_str)
    else:
        logger.warning("外部用DBは除外イベントのため更新しません: %s", after.name)

    internal_target = await find_event_page(NOTION_EVENT_INTERNAL_DB_ID, after_id_str)

    new_name = after.name
    new_content = after.description or "(内容なし)"
    new_date_iso = to_jst_iso(after.start_time)
    new_location = get_event_location(after)
    new_end_time = after.end_time or (after.start_time + timedelta(hours=1))

    google_event_id = None
    if internal_target:
        internal_page = await notion_get_event(internal_target["id"])
        google_event_id = get_google_event_id_from_notion_page(internal_page)
    if google_event_id:
        google_updated = google_update_event(
            google_event_id=google_event_id,
            name=new_name,
            description=new_content,
            start_dt=after.start_time,
            end_dt=new_end_time,
            location=new_location,
        )
        if google_updated:
            logger.info("Discordイベント更新 -> Googleカレンダー更新: %s", new_name)
        else:
            logger.error("Googleカレンダー イベント更新に失敗しました。")
    else:
        logger.warning("GoogleイベントIDが見つからないためGoogle更新をスキップします: %s", new_name)

    if target:
        page_id = target["id"]
        ok = await notion_update_event(
            page_id,
            name=new_name,
            content=new_content,
            date_iso=new_date_iso,
        )
        if ok:
            logger.info("Discordイベント更新 -> 外部用Notion更新: %s", new_name)
        else:
            logger.error("外部用Notion イベント更新に失敗しました。")
    else:
        if NOTION_EVENT_EXTERNAL_DB_ID and not is_ignored_event(after.name):
            logger.warning("外部用Notion 側に対応するイベントページが見つかりません。")

    if internal_target:
        page_id = internal_target["id"]
        ok = await notion_update_event(
            page_id,
            name=new_name,
            content=new_content,
            date_iso=new_date_iso,
            event_url=event_url,
            location=new_location,
        )
        if ok:
            logger.info("Discordイベント更新 -> 内部用Notion更新: %s", new_name)
        else:
            logger.error("内部用Notion イベント更新に失敗しました。")
    else:
        if NOTION_EVENT_INTERNAL_DB_ID:
            logger.warning("内部用Notion 側に対応するイベントページが見つかりません。")


async def handle_scheduled_event_delete(bot, event):
    eid = str(event.id)
    if is_bot_created_scheduled_event(bot, event):
        logger.info("Bot作成イベントのためDiscord削除同期をスキップ: %s", event.name)
        return

    if not is_ignored_event(event.name):
        target = await find_event_page(NOTION_EVENT_EXTERNAL_DB_ID, eid)
        if target:
            if await notion_delete_event(target["id"]):
                logger.info("Discordイベント削除 -> 外部用Notion削除: %s", event.name)
            else:
                logger.error("外部用Notion イベント削除に失敗しました。")
        else:
            if NOTION_EVENT_EXTERNAL_DB_ID:
                logger.warning("外部用の削除対象Notionイベントが見つかりません。")
    else:
        logger.warning("外部用DBは除外イベントの削除は無視します: %s", event.name)

    internal_target = await find_event_page(NOTION_EVENT_INTERNAL_DB_ID, eid)
    if internal_target:
        internal_page = await notion_get_event(internal_target["id"])
        google_event_id = get_google_event_id_from_notion_page(internal_page)
        if google_event_id:
            deleted = google_delete_event(google_event_id)
            if deleted:
                logger.info("Discordイベント削除 -> Googleカレンダー削除: %s", event.name)
            else:
                logger.error("Googleカレンダー イベント削除に失敗しました。")
        else:
            logger.warning("GoogleイベントIDが見つからないためGoogle削除をスキップします: %s", event.name)
        if await notion_delete_event(internal_target["id"]):
            logger.info("Discordイベント削除 -> 内部用Notion削除: %s", event.name)
        else:
            logger.error("内部用Notion イベント削除に失敗しました。")
    else:
        if NOTION_EVENT_INTERNAL_DB_ID:
            logger.warning("内部用の削除対象Notionイベントが見つかりません。")
