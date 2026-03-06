from datetime import datetime, timedelta, timezone

import discord
from discord.ext import commands, tasks

from config import (
    DISCORD_TOKEN,
    ENABLE_REALTIME_SYNC,
    REMINDER_CHANNEL_ID,
    REMINDER_ROLE_ID,
    REMINDER_WINDOW_MINUTES,
    logger,
)
from notify import send_day_before_reminder, send_qa_notification
from sync_discord import (
    handle_scheduled_event_create,
    handle_scheduled_event_delete,
    handle_scheduled_event_update,
)
from sync_google import validate_google_calendar_connection
from sync_notion import (
    delete_past_events,
    ensure_question_numbers,
    get_answer,
    get_qa_changes,
    load_cache,
    load_reminder_cache,
    save_cache,
    save_reminder_cache,
)


FIRST_QA_RUN = True


@tasks.loop(hours=24)
async def auto_clean():
    await delete_past_events()


@tasks.loop(hours=6)
async def auto_check_qa(bot: commands.Bot):
    global FIRST_QA_RUN

    await ensure_question_numbers()
    changes = await get_qa_changes()

    if FIRST_QA_RUN:
        logger.info("Skipping QA notifications on first run.")
        cache = load_cache()
        save_cache(cache, first_run_flag=False)
        FIRST_QA_RUN = False
        return

    for ctype, page in changes:
        if get_answer(page) == "(回答なし)":
            await send_qa_notification(bot, ctype, page)


@tasks.loop(minutes=10)
async def auto_day_before_reminder(bot: commands.Bot):
    if REMINDER_CHANNEL_ID == 0 or REMINDER_ROLE_ID == 0:
        return

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc + timedelta(hours=24)
    window_end = window_start + timedelta(minutes=max(1, REMINDER_WINDOW_MINUTES))

    cache = load_reminder_cache()
    cache_changed = False

    for guild in bot.guilds:
        try:
            events = await guild.fetch_scheduled_events()
        except Exception:
            events = list(getattr(guild, "scheduled_events", []))

        for event in events:
            start_time = getattr(event, "start_time", None)
            if not start_time:
                continue
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)

            if not (window_start <= start_time < window_end):
                continue

            event_id = str(event.id)
            if event_id in cache:
                continue

            sent = await send_day_before_reminder(bot, event)
            if sent:
                cache[event_id] = now_utc.isoformat()
                cache_changed = True

    if cache_changed:
        save_reminder_cache(cache)


intents = discord.Intents.default()
intents.guild_scheduled_events = True
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    global FIRST_QA_RUN

    logger.info("Bot Ready as %s", bot.user)

    cache = load_cache()
    FIRST_QA_RUN = cache.get("_first_qa_run", True)

    logger.info("FIRST_QA_RUN = %s", FIRST_QA_RUN)
    logger.info("ENABLE_REALTIME_SYNC = %s", ENABLE_REALTIME_SYNC)
    validate_google_calendar_connection()

    await ensure_question_numbers()

    if not auto_clean.is_running():
        auto_clean.start()

    if not auto_check_qa.is_running():
        auto_check_qa.start(bot)

    if not auto_day_before_reminder.is_running():
        auto_day_before_reminder.start(bot)

    logger.info("All background tasks started.")


@bot.event
async def on_scheduled_event_create(event):
    if not ENABLE_REALTIME_SYNC:
        return
    await handle_scheduled_event_create(bot, event)


@bot.event
async def on_scheduled_event_update(before, after):
    if not ENABLE_REALTIME_SYNC:
        return
    await handle_scheduled_event_update(bot, before, after)


@bot.event
async def on_scheduled_event_delete(event):
    if not ENABLE_REALTIME_SYNC:
        return
    await handle_scheduled_event_delete(bot, event)


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
