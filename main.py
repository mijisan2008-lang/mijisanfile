import asyncio
import logging
import sqlite3
import os
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

import aiohttp
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Message
from telegram.error import TelegramError, RetryAfter, TimedOut
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes
)

# ========= CONFIG =========
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMINS = [8281945483]
OWNER_URL = "https://t.me/firejisanvip"
BOT_NAME = "JISAN LIKE BOT"
API_URL = "https://mijisanfile-production.up.railway.app/like"

DAILY_LIMIT = 1
COOLDOWN = 5

cooldowns: Dict[int, float] = {}
DB = "bot/bot.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

_session: Optional[aiohttp.ClientSession] = None


async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        _session = aiohttp.ClientSession(timeout=timeout)
    return _session


# ========= DATABASE =========
def _init_db_sync():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    with sqlite3.connect(DB) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, date TEXT, count INTEGER, premium INTEGER, total_likes INTEGER DEFAULT 0)")
        cur.execute("CREATE TABLE IF NOT EXISTS groups(id INTEGER PRIMARY KEY)")
        cur.execute("CREATE TABLE IF NOT EXISTS stats(id INTEGER PRIMARY KEY, total INTEGER)")
        cur.execute("INSERT OR IGNORE INTO stats VALUES(1,0)")
        conn.commit()


async def init_db():
    await asyncio.to_thread(_init_db_sync)


_user_cache: Dict[int, Tuple[float, Dict[str, Any]]] = {}


async def get_user(uid: int) -> Dict[str, Any]:
    now = datetime.now().timestamp()
    if uid in _user_cache:
        expiry, data = _user_cache[uid]
        if now < expiry:
            return data.copy()

    today = datetime.now().strftime("%Y-%m-%d")

    def _fetch():
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA journal_mode=WAL")
            cur = c.cursor()
            cur.execute("SELECT date, count, premium, total_likes FROM users WHERE id=?", (uid,))
            row = cur.fetchone()
            if not row:
                cur.execute("INSERT INTO users VALUES(?,?,?,?,?)", (uid, today, 0, 0, 0))
                c.commit()
                return {"count": 0, "premium": 0, "total_likes": 0}
            elif row[0] != today:
                cur.execute("UPDATE users SET date=?, count=0 WHERE id=?", (today, uid))
                c.commit()
                return {"count": 0, "premium": row[2], "total_likes": row[3]}
            else:
                return {"count": row[1], "premium": row[2], "total_likes": row[3]}

    result = await asyncio.to_thread(_fetch)
    _user_cache[uid] = (now + 30, result.copy())
    return result


async def add_count(uid: int, likes: int) -> Tuple[int, int]:
    def _update():
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA journal_mode=WAL")
            cur = c.cursor()
            cur.execute("UPDATE users SET count = count + 1, total_likes = total_likes + ? WHERE id=?", (likes, uid))
            cur.execute("UPDATE stats SET total = total + 1 WHERE id=1")
            c.commit()
            cur.execute("SELECT count, total_likes FROM users WHERE id=?", (uid,))
            return cur.fetchone()

    new_count, new_total = await asyncio.to_thread(_update)
    if uid in _user_cache:
        del _user_cache[uid]
    return new_count, new_total


async def is_group_allowed(chat_id: int) -> bool:
    def _check():
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA journal_mode=WAL")
            cur = c.cursor()
            cur.execute("SELECT id FROM groups WHERE id=?", (chat_id,))
            return cur.fetchone() is not None
    return await asyncio.to_thread(_check)


# ========= DESIGN HELPERS =========
LINE = "━━━━━━━━━━━━━━━━━━━━━━━━━"
THIN = "─────────────────────────"

def header(title: str, icon: str = "🔥") -> str:
    return (
        f"╔{'═' * 27}╗\n"
        f"║  {icon}  {title:<20} {icon}  ║\n"
        f"╚{'═' * 27}╝"
    )

def build_card(title: str, icon: str, rows: list) -> str:
    lines = [f"*{icon} {title}*", f"`{LINE}`"]
    for row in rows:
        lines.append(row)
    lines.append(f"`{LINE}`")
    return "\n".join(lines)

def get_user_badge(uid: int, premium: bool) -> str:
    if uid in ADMINS:
        return "👑 OWNER"
    elif premium:
        return "💎 PREMIUM"
    else:
        return "👤 FREE"

def main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👑 OWNER", url=OWNER_URL),
         InlineKeyboardButton("📢 CHANNEL", url=OWNER_URL)]
    ])

def like_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔥 SEND MORE LIKES", callback_data="noop")],
        [InlineKeyboardButton("👑 OWNER", url=OWNER_URL)]
    ])


# ========= RESPONSE HELPER =========
async def finalize_response(
    processing_msg: Message,
    text: str,
    parse_mode: str = "Markdown",
    reply_markup: Optional[InlineKeyboardMarkup] = None
) -> None:
    max_retries = 2
    chat_id = processing_msg.chat_id
    bot = processing_msg.get_bot()

    async def _send_new():
        for attempt in range(max_retries):
            try:
                return await processing_msg.reply_text(
                    text, parse_mode=parse_mode, reply_markup=reply_markup
                )
            except (RetryAfter, TimedOut) as e:
                wait = e.retry_after if isinstance(e, RetryAfter) else 0.5 * (2 ** attempt)
                await asyncio.sleep(wait)
            except Exception as e:
                logger.error(f"Failed to send new message: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(0.2)
        return None

    for attempt in range(max_retries):
        try:
            await processing_msg.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
            return
        except (RetryAfter, TimedOut) as e:
            wait = e.retry_after if isinstance(e, RetryAfter) else 0.5 * (2 ** attempt)
            await asyncio.sleep(wait)
        except TelegramError as e:
            logger.warning(f"Edit failed: {e}")
            try:
                await processing_msg.delete()
            except Exception:
                pass
            try:
                await _send_new()
            except Exception:
                await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
            return
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if attempt == max_retries - 1:
                try:
                    await processing_msg.delete()
                except Exception:
                    pass
                try:
                    await _send_new()
                except Exception:
                    await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
                return
            await asyncio.sleep(0.2)


# ========= API FETCH =========
async def fetch_with_retry(uid: str, server: str, retries: int = 2) -> Dict[str, Any]:
    url = f"{API_URL}?uid={uid}&server_name={server}"
    session = await get_session()

    for attempt in range(1, retries + 1):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict) and "LikesbeforeCommand" in data:
                        return data
                    else:
                        logger.warning(f"Unexpected API response: {data}")
                        return {"error": "invalid"}
                else:
                    try:
                        err_data = await resp.json(content_type=None)
                        api_msg = err_data.get("error", "") if isinstance(err_data, dict) else ""
                    except Exception:
                        api_msg = ""

                    if "player info" in api_msg.lower() or "retrieve" in api_msg.lower():
                        return {"error": "uid_not_found"}
                    elif "token" in api_msg.lower():
                        return {"error": "no_tokens"}
                    elif attempt < retries:
                        await asyncio.sleep(0.3 * attempt)
                        continue
                    else:
                        return {"error": "http_error", "status": resp.status, "msg": api_msg}
        except asyncio.TimeoutError:
            if attempt < retries:
                await asyncio.sleep(0.3 * attempt)
                continue
            return {"error": "timeout"}
        except Exception as e:
            logger.error(f"Fetch error (attempt {attempt}): {e}")
            if attempt < retries:
                await asyncio.sleep(0.3 * attempt)
                continue
            return {"error": "failed"}
    return {"error": "failed"}


# ========= HANDLERS =========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    name = user.first_name or "Player"
    text = (
        f"🔥 *{BOT_NAME}* 🔥\n"
        f"`{LINE}`\n"
        f"👋 Welcome, *{name}*!\n\n"
        f"⚡ *Send likes to any Free Fire player*\n"
        f"instantly with just one command!\n\n"
        f"`{THIN}`\n"
        f"📌 *HOW TO USE:*\n"
        f"➤ `/like <server> <uid>`\n\n"
        f"🌍 *Servers:* `bd` `ind` `sg` `br` `vn`\n\n"
        f"📖 *Example:*\n"
        f"➤ `/like bd 6272205696`\n\n"
        f"`{THIN}`\n"
        f"🎟️ *Daily Limit:* {DAILY_LIMIT} like/day\n"
        f"⏱️ *Cooldown:* {COOLDOWN} seconds\n"
        f"`{LINE}`\n"
        f"💎 _Powered by {BOT_NAME}_"
    )
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_keyboard())


async def like(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    processing_msg = None

    try:
        # Group check — only private or approved groups
        if update.effective_chat.type != "private":
            if not await is_group_allowed(chat_id) and user_id not in ADMINS:
                await update.message.reply_text(
                    f"❌ *এই group এ bot allowed না!*\n\n"
                    f"Owner কে contact করো: {OWNER_URL}",
                    parse_mode="Markdown"
                )
                return

        args = context.args
        if len(args) != 2:
            await update.message.reply_text(
                f"⚠️ *Wrong Format!*\n\n"
                f"✅ *Correct:* `/like bd 6272205696`\n"
                f"🌍 *Servers:* `bd` `ind` `sg` `br` `vn`",
                parse_mode="Markdown"
            )
            return

        server, target = args[0].lower(), args[1]

        if not target.isdigit():
            await update.message.reply_text(
                "❌ *Invalid UID!* শুধু numbers দাও।",
                parse_mode="Markdown"
            )
            return

        # Cooldown check
        now = datetime.now().timestamp()
        if user_id in cooldowns and now - cooldowns[user_id] < COOLDOWN:
            remaining_cd = int(COOLDOWN - (now - cooldowns[user_id])) + 1
            await update.message.reply_text(
                f"⏳ *Cooldown!*\n\n"
                f"🕐 *{remaining_cd}* সেকেন্ড পর আবার try করো।",
                parse_mode="Markdown"
            )
            return
        cooldowns[user_id] = now

        # Daily limit check
        user_data = await get_user(user_id)
        if user_id not in ADMINS and not user_data["premium"]:
            if DAILY_LIMIT != 0 and user_data["count"] >= DAILY_LIMIT:
                await update.message.reply_text(
                    f"🚫 *Daily Limit Reached!*\n\n"
                    f"`{THIN}`\n"
                    f"📅 তোমার আজকের {DAILY_LIMIT}টি like শেষ।\n"
                    f"🌅 *কাল আবার try করো!*\n\n"
                    f"💎 Premium নিলে unlimited likes পাবে।\n"
                    f"`{THIN}`",
                    parse_mode="Markdown",
                    reply_markup=main_keyboard()
                )
                return

        # Processing message
        processing_msg = await update.message.reply_text(
            f"⚡ *Processing...*\n\n"
            f"🎮 UID: `{target}`\n"
            f"🌍 Server: `{server.upper()}`\n\n"
            f"_Please wait..._",
            parse_mode="Markdown"
        )

        # API call
        result = await fetch_with_retry(target, server)

        if not result or "error" in result:
            error_map = {
                "timeout":      "⏱️ *Timeout!* Server respond করেনি। আবার try করো।",
                "failed":       "📡 *Server Unreachable!* কিছুক্ষণ পর try করো।",
                "http_error":   f"⚠️ *API Error {result.get('status', '')}!* আবার try করো।",
                "invalid":      "🔄 *Invalid Response!* আবার try করো।",
                "uid_not_found":"❌ *Player Not Found!*\n\nUID টা ঠিক আছে কি? Check করো।",
                "no_tokens":    "🔐 *Server Busy!*\n\nএই server এ এখন tokens নেই।\nকিছুক্ষণ পর আবার try করো।"
            }
            error_msg = error_map.get(result.get("error", ""), "❌ *Unknown Error!* Support এ যোগাযোগ করো।")
            await finalize_response(processing_msg, error_msg, reply_markup=main_keyboard())
            return

        before   = result.get("LikesbeforeCommand", "N/A")
        sent     = result.get("LikesGivenByAPI", "N/A")
        after    = result.get("LikesafterCommand", "N/A")
        name     = result.get("PlayerNickname", "Unknown")
        api_status = result.get("status", 1)

        try:
            likes_sent = int(sent) if str(sent).isdigit() else 0
        except (ValueError, TypeError):
            likes_sent = 0

        new_daily_count, new_total_likes = await add_count(user_id, likes_sent)

        if user_id in ADMINS or user_data["premium"] or DAILY_LIMIT == 0:
            remain = "♾️ Unlimited"
        else:
            rem = max(0, DAILY_LIMIT - new_daily_count)
            remain = f"{rem} remaining"

        badge = get_user_badge(user_id, bool(user_data["premium"]))

        if api_status == 2:
            text_result = (
                f"⚠️ *LIKE ALREADY MAXED*\n"
                f"`{LINE}`\n"
                f"👤 *Name :* `{name}`\n"
                f"🆔 *UID   :* `{target}`\n"
                f"🌍 *Server:* `{server.upper()}`\n"
                f"`{THIN}`\n"
                f"💔 এই player এর like limit already full!\n"
                f"👍 *Current Likes:* `{after}`\n"
                f"`{THIN}`\n"
                f"🎟️ *Today's Limit:* {remain}\n"
                f"`{LINE}`\n"
                f"💎 _{BOT_NAME}_"
            )
        else:
            text_result = (
                f"✅ *LIKE SENT SUCCESSFULLY!*\n"
                f"`{LINE}`\n"
                f"👤 *Name  :* `{name}`\n"
                f"🆔 *UID   :* `{target}`\n"
                f"🌍 *Server:* `{server.upper()}`\n"
                f"🏷️ *Type  :* {badge}\n"
                f"`{THIN}`\n"
                f"📊 *LIKE DETAILS*\n"
                f"👍 *Before :* `{before}`\n"
                f"🔥 *Sent   :* `{sent}`\n"
                f"🏆 *After  :* `{after}`\n"
                f"`{THIN}`\n"
                f"🎟️ *Today  :* {remain}\n"
                f"💖 *Total  :* {new_total_likes} likes sent\n"
                f"`{LINE}`\n"
                f"💎 _{BOT_NAME}_"
            )

        await finalize_response(processing_msg, text_result, reply_markup=like_keyboard())

    except Exception as e:
        logger.exception("Unhandled exception in like handler")
        error_text = "⚠️ Unexpected error! Please try again later."
        if processing_msg:
            try:
                await finalize_response(processing_msg, error_text)
            except Exception:
                pass
        else:
            try:
                await update.message.reply_text(error_text)
            except Exception:
                pass


async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    text = (
        f"👑 *ADMIN PANEL*\n"
        f"`{LINE}`\n"
        f"⚙️ *User Management:*\n"
        f"  `/setlimit <num>` — daily limit set\n"
        f"  `/addpremium <id>` — premium দাও\n\n"
        f"🏘️ *Group Management:*\n"
        f"  `/addgroup <gid>` — group approve\n"
        f"  `/removegroup <gid>` — group remove\n"
        f"  `/groups` — all groups list\n\n"
        f"📢 *Broadcast:*\n"
        f"  `/broadcast <msg>` — সবাইকে message\n\n"
        f"📊 *Info:*\n"
        f"  `/stats` — bot statistics\n"
        f"  `/api` — API status check\n"
        f"  `/gid` — current chat ID\n"
        f"`{LINE}`\n"
        f"💎 _{BOT_NAME}_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    global DAILY_LIMIT
    try:
        new_limit = int(context.args[0])
        DAILY_LIMIT = new_limit
        text = (
            f"✅ *Limit Updated!*\n\n"
            f"📅 Daily limit: *{'UNLIMITED ♾️' if new_limit == 0 else str(new_limit)}*"
        )
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("⚠️ Usage: `/setlimit 5`", parse_mode="Markdown")


async def addpremium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    try:
        uid = int(context.args[0])
        def _update():
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA journal_mode=WAL")
                c.execute("UPDATE users SET premium=1 WHERE id=?", (uid,))
                c.commit()
        await asyncio.to_thread(_update)
        if uid in _user_cache:
            del _user_cache[uid]
        await update.message.reply_text(
            f"💎 *Premium Added!*\n\n🆔 User `{uid}` এখন Premium।",
            parse_mode="Markdown"
        )
    except Exception:
        await update.message.reply_text("⚠️ Error: valid user ID দাও।")


async def addgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        await update.message.reply_text("❌ Admin only!")
        return
    try:
        gid = int(context.args[0].strip())
        def _add():
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA journal_mode=WAL")
                cur = c.cursor()
                cur.execute("SELECT id FROM groups WHERE id=?", (gid,))
                if cur.fetchone():
                    return False
                cur.execute("INSERT INTO groups VALUES(?)", (gid,))
                c.commit()
                return True
        added = await asyncio.to_thread(_add)
        if added:
            await update.message.reply_text(f"✅ *Group Approved!*\n\n🆔 `{gid}`", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"⚠️ Group `{gid}` already exists!", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def removegroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        await update.message.reply_text("❌ Admin only!")
        return
    try:
        gid = int(context.args[0])
        def _remove():
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA journal_mode=WAL")
                cur = c.cursor()
                cur.execute("DELETE FROM groups WHERE id=?", (gid,))
                deleted = cur.rowcount
                c.commit()
                return deleted
        deleted = await asyncio.to_thread(_remove)
        if deleted:
            await update.message.reply_text(f"✅ *Group Removed!*\n\n🆔 `{gid}`", parse_mode="Markdown")
        else:
            await update.message.reply_text("⚠️ Group not found!", parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("⚠️ Usage: `/removegroup -100xxxxx`", parse_mode="Markdown")


async def groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    def _fetch():
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA journal_mode=WAL")
            return [str(row[0]) for row in c.execute("SELECT id FROM groups").fetchall()]
    data = await asyncio.to_thread(_fetch)
    if not data:
        await update.message.reply_text("📋 কোনো approved group নেই।")
    else:
        group_list = "\n".join([f"• `{g}`" for g in data])
        await update.message.reply_text(
            f"🏘️ *Approved Groups ({len(data)}):*\n\n{group_list}",
            parse_mode="Markdown"
        )


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return
    if not context.args:
        await update.message.reply_text("⚠️ Usage: `/broadcast <message>`", parse_mode="Markdown")
        return
    msg = " ".join(context.args)
    def _get_users():
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA journal_mode=WAL")
            return [row[0] for row in c.execute("SELECT id FROM users").fetchall()]
    users = await asyncio.to_thread(_get_users)
    sent_count = 0
    for uid in users:
        try:
            await context.bot.send_message(uid, msg)
            sent_count += 1
        except Exception:
            pass
    await update.message.reply_text(
        f"📢 *Broadcast Done!*\n\n✅ {sent_count}/{len(users)} users কে message গেছে।",
        parse_mode="Markdown"
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    def _fetch():
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA journal_mode=WAL")
            user_count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            total_likes = c.execute("SELECT total FROM stats WHERE id=1").fetchone()[0]
            premium_count = c.execute("SELECT COUNT(*) FROM users WHERE premium=1").fetchone()[0]
            group_count = c.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
            return user_count, total_likes, premium_count, group_count
    users, total, premium, grps = await asyncio.to_thread(_fetch)
    text = (
        f"📊 *BOT STATISTICS*\n"
        f"`{LINE}`\n"
        f"👥 *Total Users  :* `{users}`\n"
        f"💎 *Premium Users:* `{premium}`\n"
        f"🏘️ *Groups       :* `{grps}`\n"
        f"`{THIN}`\n"
        f"🔥 *Likes Sent   :* `{total}`\n"
        f"📅 *Daily Limit  :* `{DAILY_LIMIT if DAILY_LIMIT else '∞'}`\n"
        f"`{LINE}`\n"
        f"💎 _{BOT_NAME}_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def api(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 *Checking API status...*", parse_mode="Markdown")
    try:
        session = await get_session()
        async with session.get(API_URL) as r:
            if r.status == 200:
                await finalize_response(
                    msg,
                    f"🟢 *API ONLINE*\n\n✅ Server is working fine!\n\n💎 _{BOT_NAME}_"
                )
            else:
                await finalize_response(
                    msg,
                    f"🔴 *API OFFLINE*\n\n❌ Server is down! Status: {r.status}\n\n💎 _{BOT_NAME}_"
                )
    except Exception:
        await finalize_response(msg, f"🔴 *API OFFLINE*\n\n❌ Could not reach server.\n\n💎 _{BOT_NAME}_")


async def remain(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    data = await get_user(uid)
    badge = get_user_badge(uid, bool(data["premium"]))
    if uid in ADMINS or DAILY_LIMIT == 0:
        rem_text = "♾️ Unlimited"
    else:
        rem = max(0, DAILY_LIMIT - data["count"])
        rem_text = f"*{rem}* / {DAILY_LIMIT}"
    text = (
        f"🎟️ *DAILY LIMIT STATUS*\n"
        f"`{LINE}`\n"
        f"🏷️ *Type    :* {badge}\n"
        f"📅 *Today   :* {rem_text}\n"
        f"💖 *All Time:* `{data['total_likes']}` likes\n"
        f"`{LINE}`\n"
        f"💎 _{BOT_NAME}_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def auto_detect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip() if update.message.text else ""
    if text.isdigit() and len(text) >= 8:
        await update.message.reply_text(
            f"💡 *UID detect হয়েছে!*\n\n"
            f"Like পাঠাতে এভাবে লিখো:\n"
            f"➤ `/like bd {text}`\n\n"
            f"🌍 *Servers:* `bd` `ind` `sg` `br` `vn`",
            parse_mode="Markdown"
        )


async def gid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.message.reply_text(
        f"🆔 *Chat ID:* `{chat.id}`\n"
        f"📛 *Type:* `{chat.type}`",
        parse_mode="Markdown"
    )


# ========= ERROR HANDLER =========
async def error_handler(update: Optional[Update], context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception while handling an update:", exc_info=context.error)
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ Internal error occurred. Please try again."
            )
        except Exception:
            pass


# ========= SHUTDOWN =========
async def shutdown(application: Application):
    global _session
    if _session and not _session.closed:
        await _session.close()
    logger.info("Bot shut down gracefully.")


# ========= MAIN =========
def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is not set!")
        return

    _init_db_sync()
    logger.info("Database initialized")

    app = Application.builder()\
        .token(BOT_TOKEN)\
        .connect_timeout(30)\
        .read_timeout(30)\
        .build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("like", like))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("setlimit", setlimit))
    app.add_handler(CommandHandler("addpremium", addpremium))
    app.add_handler(CommandHandler("addgroup", addgroup))
    app.add_handler(CommandHandler("removegroup", removegroup))
    app.add_handler(CommandHandler("groups", groups))
    app.add_handler(CommandHandler("gid", gid))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("api", api))
    app.add_handler(CommandHandler("remain", remain))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, auto_detect))
    app.add_error_handler(error_handler)
    app.post_shutdown = shutdown

    logger.info(f"✨ {BOT_NAME} RUNNING")
    app.run_polling()


if __name__ == "__main__":
    main()
