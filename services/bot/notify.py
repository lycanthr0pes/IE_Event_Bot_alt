import discord
from discord.ext import commands

from config import (
    QA_CHANNEL_ID,
    REMINDER_CHANNEL_ID,
    REMINDER_ROLE_ID,
    logger,
    to_jst_iso,
)
from sync_discord import get_event_url
from sync_notion import get_answer, get_question


async def send_qa_notification(bot: commands.Bot, ctype: str, page: dict):
    if QA_CHANNEL_ID == 0:
        return

    ch = await bot.fetch_channel(QA_CHANNEL_ID)
    number = page["properties"]["質問番号"]["number"]
    number_display = number if number is not None else "?"
    q = get_question(page)
    a = get_answer(page)

    if ctype == "new":
        msg = (
            f"🆕 **新しい質問 (#{number_display}) が追加されました！**\n"
            f"**質問:** {q}\n"
            f"**回答:** {a}"
        )
    else:
        msg = (
            f"✏️ **質問 (#{number_display}) が更新されました。**\n"
            f"**質問:** {q}\n"
            f"**回答:** {a}"
        )
    await ch.send(msg)


async def send_day_before_reminder(bot: commands.Bot, event) -> bool:
    if REMINDER_CHANNEL_ID == 0 or REMINDER_ROLE_ID == 0:
        return False

    channel = bot.get_channel(REMINDER_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(REMINDER_CHANNEL_ID)
        except Exception as exc:
            logger.warning("failed to fetch reminder channel: %s", exc)
            return False

    start_iso = to_jst_iso(event.start_time)
    _ = start_iso
    event_url = get_event_url(event) or ""
    msg = f"🔔 <@&{REMINDER_ROLE_ID}> 明日開催のイベントがあります 🔔\n{event_url}"

    try:
        await channel.send(
            msg,
            allowed_mentions=discord.AllowedMentions(roles=True, users=False, everyone=False),
        )
        return True
    except Exception as exc:
        logger.warning("failed to send day-before reminder: %s", exc)
        return False
