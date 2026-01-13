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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

# Fixed Regex for Web Series
SERIES_REGEX = re.compile(
    r"(.*?)[\s\.\-_]*[sS](\d{1,2})[\s\.\-_]*[eE](\d{1,3})|(.*?)[\s\.\-_]*Season[\s\.\-_]*(\d{1,2})[\s\.\-_]*Episode[\s\.\-_]*(\d{1,3})",
    re.IGNORECASE
)

# ==============================
# RENDER FIX (FLASK SERVER)
# ==============================
flask_app = Flask(__name__)
@flask_app.route('/')
def health(): return "Bot is Online and JSON Export/Import Active!"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
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
        except: return []
    return []

def get_file_info(msg):
    if msg.video:
        return msg.video.file_unique_id, msg.video.file_name or "video", msg.video.file_size
    if msg.document:
        mime = msg.document.mime_type or ""
        if "video" in mime or msg.document.file_name.lower().endswith(('.mp4', '.mkv', '.mov')):
            return msg.document.file_unique_id, msg.document.file_name or "doc", msg.document.file_size
    return None, None, None

def parse_series(caption):
    if not caption: return None # FIX: Agar caption nahi hai to series nahi hai
    match = SERIES_REGEX.search(caption)
    if match:
        raw_title = match.group(1) or match.group(4)
        if not raw_title: return None
        title = raw_title.strip().lower() # FIX: Safe stripping
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
        "⚡ **Maza Movie Mega Bot (V4 - JSON Power) Online!**\n\n"
        "**Main Features:**\n"
        "• `/sync` - Peer errors fix karne ke liye.\n"
        "• `/index_full @chat` - Puri indexing (No-caption included).\n\n"
        "**JSON Features:**\n"
        "• `/download_json <filename>` - Database file download karein.\n"
        "• `/import_json` - JSON file ko reply karke database load karein.\n\n"
        "**Others:**\n"
        "• `/status` | `/logs` | `/stop_all` | `/clean_db`"
    )

@app.on_message(filters.command("sync") & filters.user(ADMIN_ID))
async def sync_handler(client, message):
    global IS_BUSY
    if IS_BUSY: return await message.reply_text("⛔ Bot busy hai!")
    IS_BUSY = True
    status = await message.reply_text("🔄 **Syncing chats...**")
    try:
        count = 0
        async for dialog in client.get_dialogs(): count += 1
        await status.edit(f"✅ **Sync Done!** {count} chats synced.")
    finally: IS_BUSY = False

@app.on_message(filters.command("stop_all") & filters.user(ADMIN_ID))
async def stop_all(client, message):
    global STOP_TASKS, IS_BUSY
    STOP_TASKS, IS_BUSY = True, False
    await message.reply_text("🛑 **Force Stop!** Saare tasks rukh gaye.")

# --- JSON EXPORT / IMPORT ---

@app.on_message(filters.command("download_json") & filters.user(ADMIN_ID))
async def download_json_handler(client, message):
    if len(message.command) < 2: return await message.reply_text("Filename dein: `/download_json full_source.json`")
    fname = message.command[1]
    if os.path.exists(fname):
        await message.reply_document(fname, caption=f"📂 Here is your database: `{fname}`")
    else: await message.reply_text("❌ File nahi mili.")

@app.on_message(filters.command("import_json") & filters.user(ADMIN_ID))
async def import_json_handler(client, message):
    if not message.reply_to_message or not message.reply_to_message.document:
        return await message.reply_text("❌ Kisi `.json` file ko reply karke ye command dein.")
    
    doc = message.reply_to_message.document
    if not doc.file_name.endswith(".json"): return await message.reply_text("❌ Sirf `.json` file allow hai.")
    
    status = await message.reply_text("📥 **Importing database...**")
    f_path = await client.download_media(message.reply_to_message)
    
    # Replace current DB
    os.rename(f_path, doc.file_name)
    await status.edit(f"✅ **Import Complete!** `{doc.file_name}` ab active hai.")

# --- INDEXING SYSTEM ---

async def run_indexing(client, message, chat_id, filename, mode):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY: return await message.reply_text("⛔ Bot busy hai.")
    
    IS_BUSY, STOP_TASKS = True, False
    data = []
    count = 0
    status = await message.reply_text(f"🔍 **Indexing Started:** `{chat_id}`")

    try:
        async for msg in client.get_chat_history(chat_id):
            if STOP_TASKS: break
            
            f_id, f_name, f_size = get_file_info(msg)
            if not f_id: continue

            # Handling movie indexing even if caption is None
            caption = msg.caption if msg.caption else ""
            series = parse_series(caption)
            
            if mode == "movies" and series: continue
            if mode == "webseries" and not series: continue

            data.append({
                "msg_id": msg.id,
                "file_unique_id": f_id,
                "file_name": f_name,
                "file_size": f_size,
                "caption": caption,
                "series_info": series,
                "chat_id": chat_id
            })
            
            count += 1
            if count % 2000 == 0: # Faster status updates
                await status.edit(f"📂 **Indexed:** {count} files... Bot is alive.")

        save_db(data, filename)
        await status.edit(f"✅ **Indexing Finish!** Total: {count} saved in `{filename}`")

    except Exception as e:
        await message.reply_text(f"❌ Indexing Error: {str(e)}")
    finally: IS_BUSY = False

@app.on_message(filters.command(["index_full", "index_movies", "index_webseries", "index_target"]) & filters.user(ADMIN_ID))
async def index_trigger(client, message):
    if len(message.command) < 2: return await message.reply_text("Chat username dein.")
    cmd, chat = message.command[0], message.command[1]
    
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
    
    target_chat, limit = message.command[1], int(message.command[2]) if len(message.command) > 2 else 999999
    cmd = message.command[0]
    
    s_file = "full_source.json"
    if "movies" in cmd: s_file = "movies_source.json"
    elif "series" in cmd: s_file = "series_source.json"

    source_data, target_data = load_db(s_file), load_db("target_db.json")
    if not source_data: return await message.reply_text("❌ Source index khali hai!")

    t_ids = {f['file_unique_id'] for f in target_data}
    t_hashes = {(f['file_name'], f['file_size']) for f in target_data}

    # Sorting
    if "series" in cmd or "full" in cmd:
        source_data.sort(key=lambda x: (
            x['series_info']['title'] if x['series_info'] else 'zzz',
            x['series_info']['season'] if x['series_info'] else 0,
            x['series_info']['episode'] if x['series_info'] else 0
        ))
    else: source_data.reverse()

    IS_BUSY, STOP_TASKS, sent = True, False, 0
    status = await message.reply_text("📤 **Forwarding...**")

    try:
        for item in source_data:
            if STOP_TASKS or sent >= limit: break
            if item['file_unique_id'] in t_ids or (item['file_name'], item['file_size']) in t_hashes: continue

            try:
                await client.copy_message(target_chat, item['chat_id'], item['msg_id'])
                sent += 1
                if sent % 50 == 0: await status.edit(f"📤 Sent: {sent} files...")
                await asyncio.sleep(1.5)
                if sent % 100 == 0: await asyncio.sleep(25)
            except FloodWait as e: await asyncio.sleep(e.value + 5)
            except Exception: continue

        await status.edit(f"✅ **Done!** Total sent: {sent}")
    finally: IS_BUSY = False

@app.on_message(filters.command("status") & filters.user(ADMIN_ID))
async def status_handler(client, message):
    await message.reply_text(f"📊 **Uptime:** `{int(time.time() - START_TIME)}s` | **Busy:** `{IS_BUSY}`")

if __name__ == "__main__":
    Thread(target=run_web_server, daemon=True).start()
    app.run()
