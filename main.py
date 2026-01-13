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
logger = logging.getLogger("DualTurboBot")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOSS_SESSION = os.getenv("SESSION_STRING", "") # Main account
WORKER_SESSION = os.getenv("SESSION_STRING_2", "") # Speed badhane ke liye
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# Dual Clients Initialization
app = Client("BossBot", api_id=API_ID, api_hash=API_HASH, session_string=BOSS_SESSION, sleep_threshold=60)
worker = Client("WorkerBot", api_id=API_ID, api_hash=API_HASH, session_string=WORKER_SESSION, sleep_threshold=60)

# Global States
IS_BUSY = False
STOP_TASKS = False
START_TIME = time.time()

# Regex for Series
SERIES_REGEX = re.compile(
    r"(.*?)[\s\.\-_]*[sS](\d{1,2})[\s\.\-_]*[eE](\d{1,3})|(.*?)[\s\.\-_]*Season[\s\.\-_]*(\d{1,2})[\s\.\-_]*Episode[\s\.\-_]*(\d{1,3})",
    re.IGNORECASE
)

# ==============================
# RENDER FIX (FLASK)
# ==============================
flask_app = Flask(__name__)
@flask_app.route('/')
def health(): return "Dual-Session Bot is Online and Running Fast!"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host='0.0.0.0', port=port)

# ==============================
# HELPER FUNCTIONS
# ==============================

def save_db(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def load_db(filename):
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f: return json.load(f)
        except: return []
    return []

def get_file_info(msg):
    """Broad indexing to ensure NO movie is missed"""
    # Video direct check
    if msg.video:
        return msg.video.file_unique_id, msg.video.file_name or "video", msg.video.file_size
    # Document check for video extensions
    if msg.document:
        fname = (msg.document.file_name or "").lower()
        if fname.endswith(('.mp4', '.mkv', '.mov', '.avi', '.webm')) or "video" in (msg.document.mime_type or ""):
            return msg.document.file_unique_id, msg.document.file_name or "doc", msg.document.file_size
    return None, None, None

def clean_caption(caption):
    """Extra Feature: Removes links and usernames from captions"""
    if not caption: return ""
    # Remove @usernames
    caption = re.sub(r'@[A-Za-z0-9_]+', '', caption)
    # Remove http/https links
    caption = re.sub(r'https?://\S+', '', caption)
    return caption.strip()

def parse_series(caption):
    if not caption: return None
    match = SERIES_REGEX.search(caption)
    if match:
        raw_title = match.group(1) or match.group(4)
        if not raw_title: return None
        return {
            "title": raw_title.strip().lower(),
            "season": int(match.group(2) or match.group(5)),
            "episode": int(match.group(3) or match.group(6))
        }
    return None

# ==============================
# COMMANDS (Dual Sync Included)
# ==============================

@app.on_message(filters.command("start") & filters.user(ADMIN_ID))
async def start_handler(client, message):
    await message.reply_text(
        "🚀 **Maza Movie ULTRA DUAL-SESSION Bot Active!**\n\n"
        "**Main Commands:**\n"
        "• `/sync` - Dono strings ko chats dikhayein.\n"
        "• `/index_full @chat` - Super Fast Indexing (No skips).\n"
        "• `/forward_full @target` - Dual-Account Speed Forward.\n\n"
        "**Smart Tools:**\n"
        "• `/download_json <fname>` | `/import_json` (Reply to file)\n"
        "• `/clean_db` | `/status` | `/stop_all`"
    )

@app.on_message(filters.command("sync") & filters.user(ADMIN_ID))
async def sync_dual_handler(client, message):
    global IS_BUSY
    if IS_BUSY: return await message.reply_text("⛔ Bot busy hai!")
    IS_BUSY = True
    sts = await message.reply_text("🔄 **Dono Accounts Sync ho rahe hain...**")
    try:
        # Sync Boss
        async for _ in app.get_dialogs(limit=50): pass
        # Sync Worker
        async for _ in worker.get_dialogs(limit=50): pass
        await sts.edit("✅ **Dual Sync Done!** Peer errors ab nahi aayenge.")
    finally: IS_BUSY = False

@app.on_message(filters.command("stop_all") & filters.user(ADMIN_ID))
async def stop_all(client, message):
    global STOP_TASKS, IS_BUSY
    STOP_TASKS, IS_BUSY = True, False
    await message.reply_text("🛑 **Emergency Stop!** Saare parallel tasks rukh gaye.")

# --- INDEXING SYSTEM (Improved) ---

async def run_indexing(client, message, chat_id, filename, mode):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY: return await message.reply_text("⛔ Already in task.")
    IS_BUSY, STOP_TASKS = True, False
    data = []
    count = 0
    sts = await message.reply_text(f"🔍 **Deep Indexing Started:** `{chat_id}`")

    try:
        async for msg in app.get_chat_history(chat_id): # Indexing Boss account se
            if STOP_TASKS: break
            
            f_id, f_name, f_size = get_file_info(msg)
            if not f_id: continue

            caption = msg.caption or ""
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
            if count % 2000 == 0:
                await sts.edit(f"📂 **Indexed:** {count} files... (Checking every single file)")

        save_db(data, filename)
        await sts.edit(f"✅ **Indexing Successful!** {count} files saved in `{filename}`")
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

# --- DUAL FORWARDING SYSTEM (Speed Fix) ---

@app.on_message(filters.command(["forward_full", "forward_movies", "forward_series"]) & filters.user(ADMIN_ID))
async def forward_dual_trigger(client, message):
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

    # Sorting Logic
    if "series" in cmd or "full" in cmd:
        source_data.sort(key=lambda x: (
            x['series_info']['title'] if x['series_info'] else 'zzz',
            x['series_info']['season'] if x['series_info'] else 0,
            x['series_info']['episode'] if x['series_info'] else 0
        ))
    else: source_data.reverse()

    IS_BUSY, STOP_TASKS, sent = True, False, 0
    sts = await message.reply_text("📤 **Dual-Session Forwarding Started...** (Speed Boost Active)")

    try:
        for item in source_data:
            if STOP_TASKS or sent >= limit: break
            if item['file_unique_id'] in t_ids or (item['file_name'], item['file_size']) in t_hashes: continue

            # Alternate tasks between Boss and Worker for double speed
            current_app = app if sent % 2 == 0 else worker
            
            try:
                # Extra Feature: Clean captions on the fly
                new_caption = clean_caption(item['caption'])
                
                await current_app.copy_message(
                    chat_id=target_chat,
                    from_chat_id=item['chat_id'],
                    message_id=item['msg_id'],
                    caption=new_caption if new_caption else None
                )
                sent += 1
                if sent % 50 == 0: await sts.edit(f"📤 Sent: {sent} files using Dual Sessions...")
                
                # Speed delay is lower because work is distributed
                await asyncio.sleep(0.8) 
                if sent % 150 == 0: await asyncio.sleep(20) # Slightly larger batches
            except FloodWait as e:
                logger.warning(f"FloodWait on session: {e.value}s")
                await asyncio.sleep(e.value + 5)
            except Exception: continue

        await sts.edit(f"✅ **Mission Complete!** Total Forwarded: {sent}")
    finally: IS_BUSY = False

# --- JSON TOOLS (Download/Import) ---

@app.on_message(filters.command("download_json") & filters.user(ADMIN_ID))
async def download_json(client, message):
    if len(message.command) < 2: return await message.reply_text("Filename dein.")
    fname = message.command[1]
    if os.path.exists(fname): await message.reply_document(fname)
    else: await message.reply_text("❌ File nahi mili.")

@app.on_message(filters.command("import_json") & filters.user(ADMIN_ID))
async def import_json(client, message):
    if not message.reply_to_message or not message.reply_to_message.document:
        return await message.reply_text("Reply to a .json file.")
    f_path = await client.download_media(message.reply_to_message)
    os.rename(f_path, message.reply_to_message.document.file_name)
    await message.reply_text("✅ Database Imported!")

@app.on_message(filters.command("status") & filters.user(ADMIN_ID))
async def status_handler(client, message):
    up = int(time.time() - START_TIME)
    await message.reply_text(f"📊 **Dual-Session Status:**\nUptime: `{up}s` | Workers: `2 Accounts` | Busy: `{IS_BUSY}`")

@app.on_message(filters.command("clean_db") & filters.user(ADMIN_ID))
async def clean_db_handler(client, message):
    for f in ["full_source.json", "movies_source.json", "series_source.json", "target_db.json"]:
        if os.path.exists(f): os.remove(f)
    await message.reply_text("🗑️ **DB Cleaned!**")

# ==============================
# MAIN LAUNCHER
# ==============================

async def main():
    # Start Flask Health Check
    Thread(target=run_web_server, daemon=True).start()
    
    logger.info("Starting Dual-Session UserBot...")
    await app.start()
    await worker.start()
    logger.info("Boss and Worker sessions are LIVE!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
