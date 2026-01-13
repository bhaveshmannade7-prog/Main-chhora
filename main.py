import os
import asyncio
import json
import re
import logging
import time
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, RPCError
from flask import Flask
from threading import Thread

# ==============================
# LOGGING & CONFIGURATION
# ==============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MazaMovieBot")

# Environment Variables
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

app = Client(
    "MazaMovieUserBot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING
)

# Global States for Control
IS_BUSY = False
STOP_TASKS = False

# Smart Regex for Web Series (Patterns like S01E01, Season 1, etc.)
SERIES_PATTERNS = [
    r"(?i)(.*?)[\s\.\-_]*[sS](\d{1,2})[\s\.\-_]*[eE](\d{1,3})",
    r"(?i)(.*?)[\s\.\-_]*Season[\s\.\-_]*(\d{1,2})[\s\.\-_]*Episode[\s\.\-_]*(\d{1,3})",
    r"(?i)(.*?)[\s\.\-_]*S(\d{1,2})[\s\.\-_]*E(\d{1,3})"
]

# ==============================
# RENDER WEB SERVER (PORT BINDING)
# ==============================
flask_app = Flask(__name__)

@flask_app.route('/')
def health():
    return "Maza Movie UserBot is Live and Stable!"

def run_web_server():
    # Render requires port 8080 or the dynamic PORT env
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host='0.0.0.0', port=port)

# ==============================
# DATABASE & PARSING LOGIC
# ==============================

def save_db(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def load_db(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def extract_file_info(msg):
    """Sirf Video aur Document (Jo video ho) extract karta hai"""
    if msg.video:
        return msg.video.file_unique_id, msg.video.file_name or "video", msg.video.file_size
    if msg.document:
        mime = msg.document.mime_type or ""
        if "video" in mime or msg.document.file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.mov')):
            return msg.document.file_unique_id, msg.document.file_name or "doc", msg.document.file_size
    return None, None, None

def get_series_meta(caption):
    """Smartly identifies Title, Season, and Episode"""
    if not caption: return None
    for pattern in SERIES_PATTERNS:
        match = re.search(pattern, caption)
        if match:
            title = match.group(1).replace('.', ' ').strip().lower()
            season = int(match.group(2))
            episode = int(match.group(3))
            return {"title": title, "season": season, "episode": episode}
    return None

# ==============================
# COMMANDS & TASK LOCK SYSTEM
# ==============================

@app.on_message(filters.command("start") & filters.user(ADMIN_ID))
async def start_handler(client, message):
    await message.reply_text(
        "🚀 **Maza Movie Senior Bot Active!**\n\n"
        "**Indexing (Source):**\n"
        "• `/index_full @chat` - Sab files index karein\n"
        "• `/index_movies @chat` - Only Movies\n"
        "• `/index_webseries @chat` - Only Series\n\n"
        "**Indexing (Target - For Duplicates):**\n"
        "• `/index_target_full @chat` - Target files save karein\n\n"
        "**Forwarding:**\n"
        "• `/forward_full @target [limit]`\n"
        "• `/forward_movies @target [limit]`\n"
        "• `/forward_webseries @target [limit]`\n\n"
        "**Emergency:**\n"
        "• `/stop_all` - Saare tasks turant rokne ke liye"
    )

@app.on_message(filters.command("stop_all") & filters.user(ADMIN_ID))
async def stop_tasks(client, message):
    global STOP_TASKS, IS_BUSY
    STOP_TASKS = True
    IS_BUSY = False
    await message.reply_text("🛑 **Stop Command Received!** Saare running tasks ko band kiya ja raha hai.")

# --- INDEXING SYSTEM (Uses get_chat_history) ---

async def perform_indexing(client, message, chat_id, filename, mode):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY:
        return await message.reply_text("⛔ **Bot Busy Hai!** Pehle wala kaam khatam hone dein.")
    
    IS_BUSY = True
    STOP_TASKS = False
    indexed_data = []
    count = 0
    
    status_msg = await message.reply_text(f"🔍 **Starting Indexing...**\nTarget: `{chat_id}`\nMode: `{mode}`")

    try:
        # get_chat_history is robust for 60k+ messages
        async for msg in client.get_chat_history(chat_id):
            if STOP_TASKS: break
            
            f_id, f_name, f_size = extract_file_info(msg)
            if not f_id: continue

            series_data = get_series_meta(msg.caption)
            
            # Filtering logic
            if mode == "movies" and series_data: continue
            if mode == "webseries" and not series_data: continue

            indexed_data.append({
                "msg_id": msg.id,
                "file_unique_id": f_id,
                "file_name": f_name,
                "file_size": f_size,
                "caption": msg.caption or "",
                "series_info": series_data,
                "chat_id": chat_id
            })
            
            count += 1
            if count % 1000 == 0:
                await status_msg.edit(f"📂 **Indexed:** {count} files so far...")

        save_db(indexed_data, filename)
        await status_msg.edit(f"✅ **Indexing Successful!**\nTotal Files: {count}\nDatabase: `{filename}`")

    except Exception as e:
        await message.reply_text(f"❌ **Indexing Error:** {str(e)}")
    finally:
        IS_BUSY = False

@app.on_message(filters.command(["index_full", "index_movies", "index_webseries"]) & filters.user(ADMIN_ID))
async def index_trigger(client, message):
    if len(message.command) < 2: return await message.reply_text("Channel username dein.")
    cmd = message.command[0]
    chat = message.command[1]
    
    fname = "full_source_idx.json"
    mode = "all"
    if "movies" in cmd: fname, mode = "movies_source_idx.json", "movies"
    elif "webseries" in cmd: fname, mode = "series_source_idx.json", "webseries"
    
    await perform_indexing(client, message, chat, fname, mode)

@app.on_message(filters.command("index_target_full") & filters.user(ADMIN_ID))
async def target_indexer(client, message):
    if len(message.command) < 2: return await message.reply_text("Target channel dein.")
    await perform_indexing(client, message, message.command[1], "target_db.json", "target")

# --- SMART FORWARDING SYSTEM ---

@app.on_message(filters.command(["forward_full", "forward_movies", "forward_webseries"]) & filters.user(ADMIN_ID))
async def forward_manager(client, message):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY: return await message.reply_text("⛔ **Bot Busy!**")
    
    if len(message.command) < 2: return await message.reply_text("Usage: `/forward_xxx @target_chat [limit]`")

    target_chat = message.command[1]
    limit = int(message.command[2]) if len(message.command) > 2 else 999999
    cmd = message.command[0]
    
    # Load correct database based on command
    s_file = "full_source_idx.json"
    if "movies" in cmd: s_file = "movies_source_idx.json"
    elif "webseries" in cmd: s_file = "series_source_idx.json"

    source_data = load_db(s_file)
    target_data = load_db("target_db.json")
    
    if not source_data: return await message.reply_text("❌ Source index khali hai. Pehle index karein.")

    # Duplicate check logic (Unique ID + Name/Size)
    t_ids = {f['file_unique_id'] for f in target_data}
    t_names = {(f['file_name'], f['file_size']) for f in target_data}

    # Smart Sorting for Web Series (Title -> Season -> Episode)
    if "webseries" in cmd or "full" in cmd:
        source_data.sort(key=lambda x: (
            x['series_info']['title'] if x['series_info'] else 'zzz',
            x['series_info']['season'] if x['series_info'] else 0,
            x['series_info']['episode'] if x['series_info'] else 0
        ))
    else:
        source_data.reverse() # Oldest movies first

    IS_BUSY, STOP_TASKS, sent_count = True, False, 0
    status = await message.reply_text("📤 **Starting Forwarding...**")

    try:
        for item in source_data:
            if STOP_TASKS or sent_count >= limit: break
            
            # Duplicate Skip logic
            if item['file_unique_id'] in t_ids or (item['file_name'], item['file_size']) in t_names:
                continue

            try:
                # copy_message for clean forwarding
                await client.copy_message(
                    chat_id=target_chat,
                    from_chat_id=item['chat_id'],
                    message_id=item['msg_id']
                )
                sent_count += 1
                
                # Progress every 50 files
                if sent_count % 50 == 0:
                    await status.edit(f"📤 **Sent:** {sent_count} files...")
                
                # Safety Delays to avoid Flood
                await asyncio.sleep(2) 
                if sent_count % 100 == 0:
                    await status.edit("⏳ **Batch Complete. Taking 25s break for safety...**")
                    await asyncio.sleep(25)

            except FloodWait as e:
                await asyncio.sleep(e.value + 5)
            except Exception as e:
                logger.error(f"Error skipping message: {e}")
                continue

        await status.edit(f"✅ **Forwarding Complete!** Total Forwarded: {sent_count}")
    finally:
        IS_BUSY = False

# ==============================
# MAIN STARTUP
# ==============================

if __name__ == "__main__":
    # Start Flask server for Render Health Check
    Thread(target=run_web_server, daemon=True).start()
    
    logger.info("Bot is starting...")
    app.run()
