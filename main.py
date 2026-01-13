import os
import asyncio
import json
import re
import logging
import sys
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
    sleep_threshold=60 # Auto handle small floodwaits
)

# Global States
IS_BUSY = False
STOP_TASKS = False

# Web Series Detection Regex
SERIES_REGEX = r"(?i)(.*?)[\s\.\-_]*[sS](\d{1,2})[\s\.\-_]*[eE](\d{1,3})|(?i)(.*?)[\s\.\-_]*Season[\s\.\-_]*(\d{1,2})[\s\.\-_]*Episode[\s\.\-_]*(\d{1,3})"

# ==============================
# RENDER FIX (Flask Server)
# ==============================
flask_app = Flask(__name__)

@flask_app.route('/')
def health():
    return "Maza Movie Mega Bot is 100% Active!"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Flask server starting on port {port}")
    flask_app.run(host='0.0.0.0', port=port)

# ==============================
# HELPER FUNCTIONS
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
    """Filters Video and Documents only"""
    if msg.video:
        return msg.video.file_unique_id, msg.video.file_name or "video", msg.video.file_size
    if msg.document:
        mime = msg.document.mime_type or ""
        if "video" in mime or msg.document.file_name.lower().endswith(('.mp4', '.mkv', '.mov')):
            return msg.document.file_unique_id, msg.document.file_name or "doc", msg.document.file_size
    return None, None, None

def parse_series(caption):
    if not caption: return None
    match = re.search(SERIES_REGEX, caption)
    if match:
        title = (match.group(1) or match.group(4)).strip().lower()
        season = int(match.group(2) or match.group(5))
        episode = int(match.group(3) or match.group(6))
        return {"title": title, "season": season, "episode": episode}
    return None

# ==============================
# CORE COMMANDS (Admin Only)
# ==============================

@app.on_message(filters.command("start") & filters.user(ADMIN_ID))
async def start_handler(client, message):
    await message.reply_text(
        "🚀 **Maza Movie Mega UserBot Ready!**\n\n"
        "**Main Fix Command:**\n"
        "• `/sync` - Use this first to fix Peer Errors!\n\n"
        "**Indexing:**\n"
        "• `/index_full @chat` | `/index_movies @chat`\n"
        "• `/index_webseries @chat` | `/index_target @chat`\n\n"
        "**Forwarding:**\n"
        "• `/forward_full @target [limit]`\n"
        "• `/forward_movies @target` | `/forward_series @target`\n\n"
        "**Extra Features:**\n"
        "• `/stats` - DB details check karein\n"
        "• `/clean_db` - Saara data delete karein\n"
        "• `/stop_all` - Kill all tasks"
    )

@app.on_message(filters.command("stop_all") & filters.user(ADMIN_ID))
async def stop_all_handler(client, message):
    global STOP_TASKS, IS_BUSY
    STOP_TASKS = True
    IS_BUSY = False
    await message.reply_text("🛑 **Stop Signal Sent!** Saare tasks rukh jayenge.")

# --- THE SYNC FIX (PEER_ID_INVALID FIX) ---

@app.on_message(filters.command("sync") & filters.user(ADMIN_ID))
async def sync_handler(client, message):
    global IS_BUSY
    if IS_BUSY: return await message.reply_text("⛔ Bot busy hai!")
    
    IS_BUSY = True
    status = await message.reply_text("🔄 **Syncing Chats...** Bot aapki chats scan kar raha hai taaki errors na aayein.")
    
    try:
        count = 0
        async for dialog in client.get_dialogs():
            count += 1
            if count % 20 == 0:
                await status.edit(f"🔄 **Syncing...** {count} chats found.")
        await status.edit(f"✅ **Sync Complete!** Bot ne {count} chats ko pehchan liya hai. Ab aap commands chala sakte hain.")
    except Exception as e:
        await message.reply_text(f"❌ Sync Error: {e}")
    finally:
        IS_BUSY = False

# --- INDEXING SYSTEM ---

async def run_indexing(client, message, chat_id, filename, mode):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY: return await message.reply_text("⛔ Task already running.")
    
    IS_BUSY = True
    STOP_TASKS = False
    data = []
    count = 0
    
    status = await message.reply_text(f"🔍 **Indexing:** `{chat_id}`\nMode: `{mode}`...")

    try:
        async for msg in client.get_chat_history(chat_id):
            if STOP_TASKS: break
            
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
                await status.edit(f"📂 Indexed {count} files...")

        save_db(data, filename)
        await status.edit(f"✅ **Indexing Done!** Total: {count} in `{filename}`")

    except PeerIdInvalid:
        await message.reply_text("❌ **Error: PEER_ID_INVALID**\nBot is chat ko nahi janta. Pehle `/sync` chalayein.")
    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")
    finally:
        IS_BUSY = False

@app.on_message(filters.command(["index_full", "index_movies", "index_webseries", "index_target"]) & filters.user(ADMIN_ID))
async def index_trigger(client, message):
    if len(message.command) < 2: return await message.reply_text("Format: `/index_xxx @chat`")
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
    if IS_BUSY: return await message.reply_text("⛔ Bot is busy!")
    
    if len(message.command) < 2: return await message.reply_text("Usage: `/forward_xxx @target_chat [limit]`")
    
    target_chat = message.command[1]
    limit = int(message.command[2]) if len(message.command) > 2 else 999999
    cmd = message.command[0]
    
    s_file = "full_source.json"
    if "movies" in cmd: s_file = "movies_source.json"
    elif "series" in cmd: s_file = "series_source.json"

    source_data = load_db(s_file)
    target_data = load_db("target_db.json")
    
    if not source_data: return await message.reply_text("❌ Source index khali hai!")

    t_ids = {f['file_unique_id'] for f in target_data}
    t_hashes = {(f['file_name'], f['file_size']) for f in target_data}

    # Smart Sorting
    if "series" in cmd or "full" in cmd:
        source_data.sort(key=lambda x: (
            x['series_info']['title'] if x['series_info'] else 'zzz',
            x['series_info']['season'] if x['series_info'] else 0,
            x['series_info']['episode'] if x['series_info'] else 0
        ))
    else:
        source_data.reverse()

    IS_BUSY, STOP_TASKS, sent = True, False, 0
    status = await message.reply_text("📤 **Forwarding Started...**")

    try:
        for item in source_data:
            if STOP_TASKS or sent >= limit: break
            if item['file_unique_id'] in t_ids or (item['file_name'], item['file_size']) in t_hashes:
                continue

            try:
                await client.copy_message(target_chat, item['chat_id'], item['msg_id'])
                sent += 1
                if sent % 50 == 0: await status.edit(f"📤 Sent: {sent} files...")
                await asyncio.sleep(1.5)
                if sent % 100 == 0: await asyncio.sleep(20)
            except FloodWait as e: await asyncio.sleep(e.value + 5)
            except PeerIdInvalid:
                await message.reply_text("❌ Error: Target chat not found. Run `/sync`.")
                break
            except Exception: continue

        await status.edit(f"✅ **Mission Complete!** Total: {sent}")
    finally:
        IS_BUSY = False

# --- EXTRA COMMANDS ---

@app.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_handler(client, message):
    full = len(load_db("full_source.json"))
    movies = len(load_db("movies_source.json"))
    series = len(load_db("series_source.json"))
    target = len(load_db("target_db.json"))
    
    await message.reply_text(
        "📊 **Current Statistics:**\n\n"
        f"• Full Source: `{full}` files\n"
        f"• Movies Only: `{movies}` files\n"
        f"• Series Only: `{series}` files\n"
        f"• Target Index: `{target}` files\n"
    )

@app.on_message(filters.command("clean_db") & filters.user(ADMIN_ID))
async def clean_db_handler(client, message):
    files = ["full_source.json", "movies_source.json", "series_source.json", "target_db.json"]
    for f in files:
        if os.path.exists(f): os.remove(f)
    await message.reply_text("🗑️ **Database Cleaned!** Saara indexed data delete ho chuka hai.")

# ==============================
# RUN BOT
# ==============================

if __name__ == "__main__":
    # Start Flask Health Check first
    Thread(target=run_web_server, daemon=True).start()
    
    logger.info("Mega UserBot starting...")
    try:
        app.run()
    except Exception as e:
        logger.error(f"Fatal Error: {e}")
        sys.exit(1)
