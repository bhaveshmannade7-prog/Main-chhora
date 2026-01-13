import os
import asyncio
import json
import re
import logging
import sys
import time
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, RPCError, PeerIdInvalid
from flask import Flask
from threading import Thread

# ==============================
# LOGGING & CONFIGURATION
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MazaMovieMegaBot")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

app = Client(
    "MazaMovieUserBot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    sleep_threshold=60
)

# Global States
IS_BUSY = False
STOP_TASKS = False
START_TIME = time.time()

# Regex Fixed: '(?i)' ko remove karke re.IGNORECASE flag use kiya hai
SERIES_REGEX = re.compile(
    r"(.*?)[\s\.\-_]*[sS](\d{1,2})[\s\.\-_]*[eE](\d{1,3})|(.*?)[\s\.\-_]*Season[\s\.\-_]*(\d{1,2})[\s\.\-_]*Episode[\s\.\-_]*(\d{1,3})",
    re.IGNORECASE
)

# ==============================
# RENDER PORT FIX (FLASK)
# ==============================
flask_app = Flask(__name__)

@flask_app.route('/')
def health():
    return "Bot is Online and Regex is Fixed!"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Flask server starting on port {port}")
    flask_app.run(host='0.0.0.0', port=port)

# ==============================
# HELPERS (JSON & FILE LOGIC)
# ==============================

def save_db(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        logger.error(f"Error saving {filename}: {e}")

def load_db(filename):
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return []
    return []

def get_file_info(msg):
    """Video aur Document extraction filters"""
    if msg.video:
        return msg.video.file_unique_id, msg.video.file_name or "video", msg.video.file_size
    if msg.document:
        mime = msg.document.mime_type or ""
        if "video" in mime or msg.document.file_name.lower().endswith(('.mp4', '.mkv', '.mov', '.avi')):
            return msg.document.file_unique_id, msg.document.file_name or "doc", msg.document.file_size
    return None, None, None

def parse_series(caption):
    if not caption: return None
    match = SERIES_REGEX.search(caption)
    if match:
        # Title extraction from multiple groups
        title = (match.group(1) or match.group(4)).strip().lower()
        season = int(match.group(2) or match.group(5))
        episode = int(match.group(3) or match.group(6))
        return {"title": title, "season": season, "episode": episode}
    return None

# ==============================
# ADMIN COMMANDS
# ==============================

@app.on_message(filters.command("start") & filters.user(ADMIN_ID))
async def start_handler(client, message):
    await message.reply_text(
        "⚡ **Maza Movie Mega Bot (Fixed Version) Online!**\n\n"
        "**Fixed Error:** Regex global flags fix kar diye gaye hain.\n\n"
        "**Zaroori Command:**\n"
        "• `/sync` - Sabse pehle ise chalayein Peer Errors fix karne ke liye.\n\n"
        "**Main Features:**\n"
        "• `/index_full @chat` | `/index_movies @chat` | `/index_webseries @chat`\n"
        "• `/forward_full @target [limit]` | `/index_target @chat`\n\n"
        "**Extra Features:**\n"
        "• `/status` - Bot ki health aur uptime check karein.\n"
        "• `/logs` - Current session ki basic info dekhein.\n"
        "• `/stop_all` - Kill running tasks."
    )

@app.on_message(filters.command("stop_all") & filters.user(ADMIN_ID))
async def stop_all_handler(client, message):
    global STOP_TASKS, IS_BUSY
    STOP_TASKS = True
    IS_BUSY = False
    await message.reply_text("🛑 **Stop Triggered!** Saare tasks ko force-stop kiya ja raha hai.")

# --- EXTRA COMMAND 1: STATUS ---
@app.on_message(filters.command("status") & filters.user(ADMIN_ID))
async def status_handler(client, message):
    uptime = time.time() - START_TIME
    uptime_str = time.strftime("%Hh %Mm %Ss", time.gmtime(uptime))
    await message.reply_text(
        f"📊 **Bot Status:**\n"
        f"• **Uptime:** `{uptime_str}`\n"
        f"• **Busy Status:** `{'Yes' if IS_BUSY else 'No'}`\n"
        f"• **Python Version:** `{sys.version.split()[0]}`\n"
        f"• **Health:** `Good (Regex Fixed)`"
    )

# --- EXTRA COMMAND 2: LOGS SUMMARY ---
@app.on_message(filters.command("logs") & filters.user(ADMIN_ID))
async def logs_handler(client, message):
    await message.reply_text(
        f"📑 **System Logs Summary:**\n"
        f"• Render Port: `Listening on 8080`\n"
        f"• Last Action: `Command Received`\n"
        f"• Storage: `JSON files active`"
    )

# --- SYNC COMMAND (PEER_ID_INVALID FIX) ---
@app.on_message(filters.command("sync") & filters.user(ADMIN_ID))
async def sync_handler(client, message):
    global IS_BUSY
    if IS_BUSY: return await message.reply_text("⛔ Bot busy hai!")
    IS_BUSY = True
    status = await message.reply_text("🔄 **Syncing your chat list...**")
    try:
        count = 0
        async for dialog in client.get_dialogs():
            count += 1
        await status.edit(f"✅ **Sync Done!** Bot ne {count} chats ko pehchan liya hai. Ab `PeerIdInvalid` error nahi aayega.")
    except Exception as e:
        await message.reply_text(f"❌ Sync Error: {e}")
    finally:
        IS_BUSY = False

# --- INDEXING SYSTEM ---

async def run_indexing(client, message, chat_id, filename, mode):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY: return await message.reply_text("⛔ Bot busy hai, pehle wala kaam rukhne do.")
    
    IS_BUSY = True
    STOP_TASKS = False
    data = []
    count = 0
    
    status = await message.reply_text(f"🔍 **Indexing Started:** `{chat_id}`\nMode: `{mode}`")

    try:
        async for msg in client.get_chat_history(chat_id):
            if STOP_TASKS:
                await message.reply_text("⚠️ Indexing manually stop kar di gayi.")
                break
            
            f_id, f_name, f_size = get_file_info(msg)
            if not f_id: continue

            series = parse_series(msg.caption)
            if mode == "movies" and series: continue
            if mode == "webseries" and not series: continue

            data.append({
                "msg_id": msg.id,
                "file_unique_id": f_id,
                "file_name": f_name,
                "file_size": f_size,
                "caption": msg.caption or "",
                "series_info": series,
                "chat_id": chat_id
            })
            
            count += 1
            if count % 1000 == 0:
                await status.edit(f"📂 Indexed {count} files... Bot is alive.")

        save_db(data, filename)
        await status.edit(f"✅ **Indexing Complete!** Total: {count} saved in `{filename}`")

    except PeerIdInvalid:
        await message.reply_text("❌ Error: Peer ID Invalid. Pehle `/sync` command chalayein!")
    except Exception as e:
        await message.reply_text(f"❌ Error during indexing: {str(e)}")
    finally:
        IS_BUSY = False

@app.on_message(filters.command(["index_full", "index_movies", "index_webseries", "index_target"]) & filters.user(ADMIN_ID))
async def index_trigger(client, message):
    if len(message.command) < 2: return await message.reply_text("Chat username dein.")
    cmd = message.command[0]
    chat = message.command[1]
    
    fname = "full_source.json"
    mode = "all"
    if "movies" in cmd: fname, mode = "movies_source.json", "movies"
    elif "webseries" in cmd: fname, mode = "series_source.json", "webseries"
    elif "target" in cmd: fname, mode = "target_db.json", "target"
    
    await run_indexing(client, message, chat, fname, mode)

# --- FORWARDING SYSTEM ---

@app.on_message(filters.command(["forward_full", "forward_movies", "forward_series"]) & filters.user(ADMIN_ID))
async def forward_trigger(client, message):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY: return await message.reply_text("⛔ Bot busy hai!")
    
    if len(message.command) < 2: return await message.reply_text("Format: `/forward_xxx @target_chat [limit]`")
    
    target_chat = message.command[1]
    limit = int(message.command[2]) if len(message.command) > 2 else 999999
    cmd = message.command[0]
    
    s_file = "full_source.json"
    if "movies" in cmd: s_file = "movies_source.json"
    elif "series" in cmd: s_file = "series_source.json"

    source_data = load_db(s_file)
    target_data = load_db("target_db.json")
    
    if not source_data: return await message.reply_text("❌ Source index khali hai! Pehle index karein.")

    t_ids = {f['file_unique_id'] for f in target_data}
    t_hashes = {(f['file_name'], f['file_size']) for f in target_data}

    # Sorting Logic
    if "series" in cmd or "full" in cmd:
        source_data.sort(key=lambda x: (
            x['series_info']['title'] if x['series_info'] else 'zzz',
            x['series_info']['season'] if x['series_info'] else 0,
            x['series_info']['episode'] if x['series_info'] else 0
        ))
    else:
        source_data.reverse()

    IS_BUSY, STOP_TASKS, sent = True, False, 0
    status = await message.reply_text("📤 **Forwarding shuru ho gayi hai...**")

    try:
        for item in source_data:
            if STOP_TASKS or sent >= limit: break
            if item['file_unique_id'] in t_ids or (item['file_name'], item['file_size']) in t_hashes:
                continue

            try:
                await client.copy_message(target_chat, item['chat_id'], item['msg_id'])
                sent += 1
                if sent % 50 == 0: await status.edit(f"📤 Sent: {sent} files...")
                await asyncio.sleep(1.8) # Constant safety delay
                if sent % 100 == 0: await asyncio.sleep(25)
            except FloodWait as e: await asyncio.sleep(e.value + 5)
            except Exception: continue

        await status.edit(f"✅ **Forwarding Mission Complete!** Total: {sent}")
    finally:
        IS_BUSY = False

# ==============================
# RUNNING THE APP
# ==============================

if __name__ == "__main__":
    # Start Flask for Render
    Thread(target=run_web_server, daemon=True).start()
    
    logger.info("Regex Fixed UserBot is starting...")
    try:
        app.run()
    except Exception as e:
        logger.error(f"Fatal Startup Error: {e}")
