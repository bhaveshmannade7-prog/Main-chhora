import os
import asyncio
import json
import re
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, RPCError
from flask import Flask
from threading import Thread

# ==============================
# CONFIGURATION & ENV VARS
# ==============================
# Render ke Dashboard me ye Vars zaroor add karna
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

# ==============================
# GLOBAL STATE & LOCKS
# ==============================
IS_BUSY = False
STOP_TASKS = False

# Smart Web Series Regex
SERIES_REGEX = r"(?i)(.*?)[\s\.\-_]*[sS](\d{1,2})[\s\.\-_]*[eE](\d{1,3})|(?i)(.*?)[\s\.\-_]*Season[\s\.\-_]*(\d{1,2})[\s\.\-_]*Episode[\s\.\-_]*(\d{1,3})"

# ==============================
# RENDER HEALTH CHECK SERVER
# ==============================
# Ye Render ke "No open ports detected" error ko fix karega
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    return "UserBot is Running 24/7!"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host='0.0.0.0', port=port)

# ==============================
# DATABASE UTILS (JSON)
# ==============================

def save_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def load_json(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def get_file_details(msg):
    """Sirf Video aur Documents extract karne ke liye"""
    if msg.video:
        return msg.video.file_unique_id, msg.video.file_name or "video.mp4", msg.video.file_size
    if msg.document:
        return msg.document.file_unique_id, msg.document.file_name or "file.mkv", msg.document.file_size
    return None, None, None

def parse_series(caption):
    """Caption se Series Title, Season, aur Episode nikalne ke liye"""
    if not caption: return None
    match = re.search(SERIES_REGEX, caption)
    if match:
        title = match.group(1) or match.group(4)
        season = int(match.group(2) or match.group(5))
        episode = int(match.group(3) or match.group(6))
        return {"title": title.strip().lower(), "season": season, "episode": episode}
    return None

# ==============================
# CORE COMMANDS
# ==============================

@app.on_message(filters.command("start") & filters.user(ADMIN_ID))
async def start_handler(client, message):
    await message.reply_text(
        "⚡ **UserBot Online Hai!**\n\n"
        "Main Commands:\n"
        "• `/index_full @channel` - Sab files index karein\n"
        "• `/forward_full @target` - Forwarding shuru karein\n"
        "• `/stop_all` - Sab kuch rokne ke liye"
    )

@app.on_message(filters.command("stop_all") & filters.user(ADMIN_ID))
async def stop_handler(client, message):
    global STOP_TASKS, IS_BUSY
    STOP_TASKS = True
    IS_BUSY = False
    await message.reply_text("🛑 **STOP command receive ho gaya!** Lock khul gaya hai.")

# ==============================
# INDEXING SYSTEM
# ==============================

@app.on_message(filters.command(["index_full", "index_movies", "index_webseries"]) & filters.user(ADMIN_ID))
async def index_logic(client, message):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY:
        return await message.reply_text("⛔ Bot pehle se ek task kar raha hai!")

    if len(message.command) < 2:
        return await message.reply_text("Usage: `/index_full @source_channel`")

    source_chat = message.command[1]
    cmd = message.command[0]
    
    # Decide filename and mode
    fname = "full_source_index.json"
    mode = "all"
    if "movies" in cmd:
        fname, mode = "movie_source_index.json", "movies"
    elif "webseries" in cmd:
        fname, mode = "webseries_source_index.json", "webseries"

    IS_BUSY = True
    STOP_TASKS = False
    data = []
    count = 0
    
    status = await message.reply_text(f"🔍 **Indexing {mode} shuru ho rahi hai...**")

    try:
        # get_chat_history use kar rahe hain high performance ke liye
        async for msg in client.get_chat_history(source_chat):
            if STOP_TASKS: break
            
            f_id, f_name, f_size = get_file_details(msg)
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
                "source_chat": source_chat
            })
            
            count += 1
            if count % 1000 == 0:
                await status.edit(f"📂 **Indexed:** {count} files...")

        save_json(data, fname)
        await status.edit(f"✅ **Indexing Khatam!**\nTotal: `{count}` files saved in `{fname}`")

    except Exception as e:
        await message.reply_text(f"❌ Error: {str(e)}")
    finally:
        IS_BUSY = False

# ==============================
# FORWARDING SYSTEM
# ==============================

@app.on_message(filters.command(["forward_full", "forward_movies", "forward_webseries"]) & filters.user(ADMIN_ID))
async def forward_logic(client, message):
    global IS_BUSY, STOP_TASKS
    if IS_BUSY: return await message.reply_text("⛔ Bot busy hai!")
    
    if len(message.command) < 2:
        return await message.reply_text("Usage: `/forward_full @target_channel`")

    target_chat = message.command[1]
    cmd = message.command[0]
    
    # Files Load Karein
    s_file = "full_source_index.json"
    t_file = "full_target_index.json"
    if "movies" in cmd:
        s_file, t_file = "movie_source_index.json", "movie_target_index.json"
    elif "webseries" in cmd:
        s_file, t_file = "webseries_source_index.json", "webseries_target_index.json"

    source_data = load_json(s_file)
    target_data = load_json(t_file)
    
    if not source_data:
        return await message.reply_text("❌ Source index khali hai!")

    # Duplicate check logic: Unique ID + (Name+Size)
    target_ids = {item['file_unique_id'] for item in target_data}
    target_names = {(item['file_name'], item['file_size']) for item in target_data}

    # Smart Web Series Sorting
    if "webseries" in cmd or "full" in cmd:
        source_data.sort(key=lambda x: (
            x['series_info']['title'] if x['series_info'] else 'zzz',
            x['series_info']['season'] if x['series_info'] else 0,
            x['series_info']['episode'] if x['series_info'] else 0
        ))

    IS_BUSY = True
    STOP_TASKS = False
    sent = 0
    status = await message.reply_text("📤 **Forwarding start ho gayi hai...**")

    try:
        for item in source_data:
            if STOP_TASKS: break
            
            # Duplicate Skip
            if item['file_unique_id'] in target_ids or (item['file_name'], item['file_size']) in target_names:
                continue

            try:
                # copy_message use kar rahe hain taaki clean file jaye
                await client.copy_message(
                    chat_id=target_chat,
                    from_chat_id=item['source_chat'],
                    message_id=item['msg_id']
                )
                sent += 1
                
                # Safety Delays
                if sent % 100 == 0:
                    await status.edit(f"✅ **Sent:** {sent} files. Break le raha hu (25s)...")
                    await asyncio.sleep(25)
                elif sent % 10 == 0:
                    await status.edit(f"📤 **Sent:** {sent} files...")
                    await asyncio.sleep(2)

            except FloodWait as e:
                await status.edit(f"⏳ **FloodWait:** Waiting for {e.value}s...")
                await asyncio.sleep(e.value + 5)
            except RPCError:
                continue

        await status.edit(f"🏆 **Forwarding Complete!**\nTotal: {sent} files sent.")
    finally:
        IS_BUSY = False

# ==============================
# MAIN START (RENDER COMPATIBLE)
# ==============================

if __name__ == "__main__":
    # Start Health Check Server for Render
    Thread(target=run_web_server, daemon=True).start()
    
    # Start Bot
    print("MazaMovie UserBot is Starting...")
    app.run()
