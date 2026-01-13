import os
import asyncio
import json
import re
from pyrogram import Client, filters, errors
from pyrogram.errors import FloodWait
from pyrogram.types import Message

# ================= CONFIGURATION =================
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

app = Client(
    "MazaMovieBot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING
)

# ================= GLOBAL STATES =================
IS_BUSY = False
STOP_TASKS = False

# DB File Names
DB_FILES = {
    "src_full": "full_source_index.json",
    "src_movie": "movie_source_index.json",
    "src_series": "webseries_source_index.json",
    "tgt_full": "full_target_index.json",
    "tgt_movie": "movie_target_index.json",
    "tgt_series": "webseries_target_index.json"
}

# ================= UTILS & HELPERS =================

def load_db(filename):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    return []

def save_db(filename, data):
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

def get_file_info(message: Message):
    """Extract file info: video or document only."""
    obj = message.video or message.document
    if not obj:
        return None
    return {
        "msg_id": message.id,
        "unique_id": obj.file_unique_id,
        "name": getattr(obj, "file_name", "None"),
        "size": obj.file_size,
        "caption": message.caption or ""
    }

def clean_title(caption):
    """Smartly cleans title for grouping."""
    if not caption: return "Unknown"
    # Remove Season/Episode patterns to get base title
    clean = re.split(r'(?i)(S\d+|Season\s*\d+|E\d+|Episode\s*\d+|Complete)', caption)[0]
    return clean.strip().lower()

def parse_series_info(caption):
    """Identify Season and Episode numbers."""
    s_match = re.search(r'(?i)S(\d+)|Season\s*(\d+)', caption)
    e_match = re.search(r'(?i)E(\d+)|Episode\s*(\d+)', caption)
    season = int(s_match.group(1) or s_match.group(2)) if s_match else 0
    episode = int(e_match.group(1) or e_match.group(2)) if e_match else 0
    is_complete = "complete" in caption.lower()
    return season, episode, is_complete

# ================= COMMAND LOCK DECORATOR =================

def lock_handler(func):
    async def wrapper(client, message):
        global IS_BUSY, STOP_TASKS
        if message.from_user.id != ADMIN_ID:
            return
        if IS_BUSY and message.text != "/stop_all":
            await message.reply("⛔ Bot busy hai, pehle wala kaam complete hone do ya `/stop_all` karo.")
            return
        await func(client, message)
    return wrapper

# ================= CORE LOGIC =================

async def perform_indexing(message, chat_id, db_key, filter_type=None):
    global IS_BUSY, STOP_TASKS
    IS_BUSY = True
    STOP_TASKS = False
    
    status = await message.reply(f"🔍 Indexing chalu ho rahi hai: `{chat_id}`...")
    data = []
    count = 0

    try:
        async for msg in app.get_chat_history(chat_id):
            if STOP_TASKS:
                await message.reply("🛑 Task manually stop kar diya gaya.")
                break
            
            file_data = get_file_info(msg)
            if not file_data:
                continue

            cap = file_data['caption'].lower()
            
            # Smart Filtering Logic
            if filter_type == "movie":
                if any(x in cap for x in ["s0", "season", "episode", "e0"]): continue
            elif filter_type == "series":
                if not any(x in cap for x in ["s0", "season", "episode", "e0", "complete"]): continue

            data.append(file_data)
            count += 1
            if count % 500 == 0:
                await status.edit(f"📂 Indexing in progress...\nFound: `{count}` files.")

        save_db(DB_FILES[db_key], data)
        await status.edit(f"✅ Indexing Complete!\nTotal Files: `{count}`\nSaved to: `{DB_FILES[db_key]}`")

    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")
    finally:
        IS_BUSY = False

# ================= FORWARDING LOGIC =================

async def smart_forwarder(message, target_chat, src_db_key, tgt_db_key, mode="full", limit=None):
    global IS_BUSY, STOP_TASKS
    IS_BUSY = True
    STOP_TASKS = False

    source_data = load_db(DB_FILES[src_db_key])
    target_data = load_db(DB_FILES[tgt_db_key])
    target_uniques = {x['unique_id'] for x in target_data}
    target_names = { (x['name'], x['size']) for x in target_data }

    if limit: source_data = source_data[:int(limit)]
    
    # Sorting for Web Series
    if mode == "series":
        source_data.sort(key=lambda x: (clean_title(x['caption']), parse_series_info(x['caption'])))

    status = await message.reply("🚀 Forwarding chalu ho rahi hai...")
    
    count = 0
    batch_count = 0

    for item in source_data:
        if STOP_TASKS: break
        
        # Duplicate Check
        if item['unique_id'] in target_uniques or (item['name'], item['size']) in target_names:
            continue

        try:
            await app.copy_message(
                chat_id=target_chat,
                from_chat_id=message.reply_to_message.chat.id if message.reply_to_message else message.chat.id, 
                message_id=item['msg_id']
            )
            count += 1
            batch_count += 1
            
            if count % 10 == 0:
                await status.edit(f"✅ Forwarding...\nDone: `{count}`\nSkipping duplicates...")

            # Safety Rules
            if batch_count >= 100:
                await status.edit("⏳ 100 files done. Taking 25s safety break...")
                await asyncio.sleep(25)
                batch_count = 0
            else:
                await asyncio.sleep(1.5) # Flood protection base delay

        except FloodWait as e:
            await asyncio.sleep(e.value + 5)
        except Exception:
            continue

    await status.edit(f"🏁 Task Finished!\nTotal Forwarded: `{count}`")
    IS_BUSY = False

# ================= HANDLERS =================

@app.on_message(filters.command("stop_all") & filters.user(ADMIN_ID))
async def stop_all_handler(_, message):
    global STOP_TASKS, IS_BUSY
    STOP_TASKS = True
    IS_BUSY = False
    await message.reply("🛑 Saare tasks rok diye gaye hain aur Lock release kar diya gaya hai.")

@app.on_message(filters.command(["index_full", "index_movies", "index_webseries"]) & filters.me)
@lock_handler
async def start_src_index(client, message):
    cmd = message.command[0]
    if len(message.command) < 2:
        return await message.reply("Format: `/index_xxx <chat_id/username>`")
    
    chat = message.command[1]
    f_type = "movie" if "movies" in cmd else "series" if "webseries" in cmd else None
    db_key = f"src_{'movie' if f_type=='movie' else 'series' if f_type=='series' else 'full'}"
    
    await perform_indexing(message, chat, db_key, f_type)

@app.on_message(filters.command(["index_target_full", "index_target_movies", "index_target_webseries"]) & filters.me)
@lock_handler
async def start_tgt_index(client, message):
    cmd = message.command[0]
    if len(message.command) < 2:
        return await message.reply("Format: `/index_target_xxx <chat_id/username>`")
    
    chat = message.command[1]
    f_type = "movie" if "movies" in cmd else "series" if "webseries" in cmd else None
    db_key = f"tgt_{'movie' if f_type=='movie' else 'series' if f_type=='series' else 'full'}"
    
    await perform_indexing(message, chat, db_key, f_type)

@app.on_message(filters.command(["forward_full", "forward_movies", "forward_webseries"]) & filters.me)
@lock_handler
async def start_forward(client, message):
    if len(message.command) < 2:
        return await message.reply("Format: `/forward_xxx <target_chat> [limit]`")
    
    cmd = message.command[0]
    target = message.command[1]
    limit = message.command[2] if len(message.command) > 2 else None
    
    mode = "movie" if "movies" in cmd else "series" if "webseries" in cmd else "full"
    src_key = f"src_{mode}"
    tgt_key = f"tgt_{mode}"
    
    await smart_forwarder(message, target, src_key, tgt_key, mode, limit)

@app.on_message(filters.command("start") & filters.me)
async def start_msg(_, message):
    await message.reply("🔥 **Maza Movie Indexer UserBot Online!**\n\nMain 60k+ files handle karne ke liye ready hu.\n\nCommands:\n- `/index_full`\n- `/index_target_full`\n- `/forward_full`\n- `/stop_all`")

# Render dummy server
async def run_bot():
    await app.start()
    print("Bot is running...")
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_bot())
