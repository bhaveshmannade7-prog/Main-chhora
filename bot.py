from keep_alive import keep_alive
keep_alive()

import os, time, re, json
import asyncio
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, 
    PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, 
    RPCError, UsernameInvalid, ChannelPrivate 
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Environment Variables ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION1 = os.getenv("SESSION1") # Boss
SESSION2 = os.getenv("SESSION2") # Worker
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# --- Dual Client Setup ---
# Boss (Commands + Forwarding)
bot = Client("boss", api_id=API_ID, api_hash=API_HASH, session_string=SESSION1)
# Worker (Only Forwarding helper)
worker = Client("worker", api_id=API_ID, api_hash=API_HASH, session_string=SESSION2)

# Compatibility: Commands are registered on 'app' (which is 'bot')
app = bot 

# --- Runtime state ---
target_channel = None
limit_messages = None
forwarded_count = 0
GLOBAL_TASK_RUNNING = False 
mode_copy = True
PER_MSG_DELAY = 0.5 
BATCH_SIZE_FOR_BREAK = 250
BREAK_DURATION_SEC = 25
locked_content = None 

# --- Database Files ---
DUPLICATE_DB_FILE = "forwarded_unique_ids.txt"
MOVIE_INDEX_DB_FILE = "movie_database.json"
TARGET_MOVIE_INDEX_DB_FILE = "target_movie_index.json"
WEBSERIES_INDEX_DB_FILE = "webseries_database.json"
TARGET_WEBSERIES_INDEX_DB_FILE = "target_webseries_index.json"
FULL_SOURCE_INDEX_DB_FILE = "full_source_index.json"
FULL_TARGET_INDEX_DB_FILE = "full_target_index.json"
BAD_QUALITY_DB_FILE = "bad_quality_movies.json" 
LOCKED_CONTENT_FILE = "locked_content.txt"
EDITING_INDEX_DB_FILE = "editing_index.json"

# In-memory sets
movie_fwd_unique_ids = set()
movie_target_compound_keys = set() 
webseries_fwd_unique_ids = set()
webseries_target_compound_keys = set() 
full_fwd_unique_ids = set()
full_target_compound_keys = set()

# Regex Patterns
BAD_QUALITY_KEYWORDS = [
    r"cam", r"camrip", r"hdcam", r"ts", r"telesync", r"tc", 
    r"\(line\)", r"\(clean\)", r"line audio", r"bad audio",
    r"screen record", r"screener", r"hq-cam"
]
BAD_QUALITY_REGEX = re.compile(r'\b(?:' + '|'.join(BAD_QUALITY_KEYWORDS) + r')\b', re.IGNORECASE)

EPISODE_PACK_REGEX = re.compile(r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(?:E|Ep|Episode)\s*(\d{1,3})\s*-\s*(\d{1,3})", re.IGNORECASE | re.DOTALL)
EPISODE_REGEX = re.compile(r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(?:E|Ep|Episode)\s*(\d{1,3})(?!.*\d)", re.IGNORECASE | re.DOTALL)
SEASON_COMPLETE_REGEX = re.compile(r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(Complete)", re.IGNORECASE | re.DOTALL)
EPISODE_ONLY_REGEX = re.compile(r"(.*?)(?:Episode|Ep)\s*(\d{1,3})(?!.*\d)", re.IGNORECASE | re.DOTALL)
SIMPLE_SEASON_REGEX = re.compile(r"\b(S\d{1,2})\b", re.IGNORECASE)
SERIES_KEYWORDS_REGEX = re.compile(r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE)
LINK_USERNAME_REGEX = re.compile(r"(?:https?://[^\s]+|t\.me/[^\s]+|@[\w]+)", re.IGNORECASE)

def get_default_movie_index():
    return {"source_channel_id": None, "source_channel_name": None, "movies": {}}
movie_index = get_default_movie_index()

# --- Database Helpers ---
def load_locked_content():
    global locked_content
    if os.path.exists(LOCKED_CONTENT_FILE):
        try:
            with open(LOCKED_CONTENT_FILE, "r", encoding="utf-8") as f:
                locked_content = f.read().strip() or None
        except: locked_content = None
    else: locked_content = None

def save_locked_content():
    global locked_content
    try:
        with open(LOCKED_CONTENT_FILE, "w", encoding="utf-8") as f:
            f.write(locked_content if locked_content else "")
    except: pass

def load_db(file_path, unique_set, compound_set):
    if os.path.exists(DUPLICATE_DB_FILE):
        try:
            with open(DUPLICATE_DB_FILE, "r") as f:
                for line in f: unique_set.add(line.strip())
        except: pass
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                unique_set.update(data.get("unique_ids", []))
                compound_set.update(data.get("compound_keys", []))
        except: pass

def load_movie_duplicate_dbs():
    global movie_fwd_unique_ids, movie_target_compound_keys
    movie_fwd_unique_ids.clear(); movie_target_compound_keys.clear()
    load_db(TARGET_MOVIE_INDEX_DB_FILE, movie_fwd_unique_ids, movie_target_compound_keys)

def load_webseries_duplicate_dbs():
    global webseries_fwd_unique_ids, webseries_target_compound_keys
    webseries_fwd_unique_ids.clear(); webseries_target_compound_keys.clear()
    load_db(TARGET_WEBSERIES_INDEX_DB_FILE, webseries_fwd_unique_ids, webseries_target_compound_keys)

def load_full_duplicate_dbs():
    global full_fwd_unique_ids, full_target_compound_keys
    full_fwd_unique_ids.clear(); full_target_compound_keys.clear()
    load_db(FULL_TARGET_INDEX_DB_FILE, full_fwd_unique_ids, full_target_compound_keys)

def save_forwarded_id(unique_id, compound_key, db_type="movie"):
    try:
        with open(DUPLICATE_DB_FILE, "a") as f: f.write(f"{unique_id}\n")
        if db_type == "movie":
            movie_fwd_unique_ids.add(unique_id)
            if compound_key: movie_target_compound_keys.add(compound_key)
        elif db_type == "webseries":
            webseries_fwd_unique_ids.add(unique_id)
            if compound_key: webseries_target_compound_keys.add(compound_key)
        elif db_type == "full":
            full_fwd_unique_ids.add(unique_id)
            if compound_key: full_target_compound_keys.add(compound_key)
    except: pass

def load_movie_index_db():
    global movie_index
    if os.path.exists(MOVIE_INDEX_DB_FILE):
        try:
            with open(MOVIE_INDEX_DB_FILE, "r") as f: movie_index = json.load(f)
        except: movie_index = get_default_movie_index()
    else: movie_index = get_default_movie_index()

def save_movie_index_db():
    try:
        with open(MOVIE_INDEX_DB_FILE, "w") as f: json.dump(movie_index, f, indent=2)
    except: pass

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

async def resolve_chat_id(client: Client, ref: str | int):
    ref_str = str(ref).strip()
    try:
        if ref_str.lstrip('-').isdigit():
            return await client.get_chat(int(ref_str))
    except: pass
    
    if bool(re.search(r"(t\.me\/\+|joinchat\/|\?startinvite=|\?invite=)", ref_str)):
        try: return await client.join_chat(ref_str)
        except UserAlreadyParticipant: return await client.get_chat(ref_str)
        except Exception as e: raise RuntimeError(f"Invite Error: {e}")

    try: return await client.get_chat(ref_str)
    except Exception as e: raise RuntimeError(f"Chat Resolve Error: {e}")

def get_media_details(m):
    media = m.video or m.document
    if not media: return None, None, None
    return getattr(media, 'file_name', None), getattr(media, 'file_size', None), getattr(media, 'file_unique_id', None)

# --- Start Command ---
START_MESSAGE = """
**🚀 Dual Engine Bot Ready!**

**Full Forward (Fastest):**
`/index_full <id>` - Index Source
`/index_target_full <id>` - Index Target
`/forward_full <id> [limit]` - Start Dual Engine Forwarding

**Movies:**
`/index <id>` | `/index_target <id>`
`/start_forward` (Uses set_target)

**Web Series:**
`/index_webseries <id>` | `/index_target_webseries <id>`
`/forward_webseries <id> [limit]`

**Tools:**
`/stop_all` - Emergency Stop
`/status` - Check Progress
`/clean_dupes <id>` - Remove Duplicates
"""
@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_cmd(_, message):
    await message.reply(START_MESSAGE)

# --- Indexing Commands (Robust Loop) ---

@app.on_message(filters.command("index") & filters.create(only_admin))
async def index_channel_cmd(_, message):
    global GLOBAL_TASK_RUNNING, movie_index
    if GLOBAL_TASK_RUNNING: return await message.reply("❌ Busy.")
    try: source_ref = message.text.split(" ", 1)[1].strip()
    except: return await message.reply("Usage: `/index <id>`")
    
    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING, movie_index
        status = await message.reply("⏳ Indexing Movies...")
        try:
            chat = await resolve_chat_id(app, source_ref)
            movie_index = get_default_movie_index()
            movie_index["source_channel_id"] = chat.id
            movie_index["source_channel_name"] = chat.title
            
            count = 0
            async for m in app.get_chat_history(chat.id):
                if not GLOBAL_TASK_RUNNING: break
                if not (m.video or m.document): continue
                
                fname, fsize, uid = get_media_details(m)
                if not uid: continue
                
                if SERIES_KEYWORDS_REGEX.search((fname or "") + " " + (m.caption or "")): continue
                
                if uid not in movie_index["movies"]:
                    movie_index["movies"][uid] = {"message_id": m.id, "file_name": fname, "file_size": fsize}
                    count += 1
                if count % 500 == 0: await status.edit(f"⏳ Found: {count}")
            
            save_movie_index_db()
            await status.edit(f"✅ Indexed {count} movies.")
        except Exception as e: await status.edit(f"Error: {e}")
        finally: GLOBAL_TASK_RUNNING = False
    app.loop.create_task(runner())

@app.on_message(filters.command("index_target") & filters.create(only_admin))
async def index_target_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING: return await message.reply("❌ Busy.")
    try: target_ref = message.text.split(" ", 1)[1].strip()
    except: return await message.reply("Usage: `/index_target <id>`")

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = await message.reply("⏳ Indexing Target...")
        try:
            chat = await resolve_chat_id(app, target_ref)
            uids = set(); ckeys = set()
            count = 0
            async for m in app.get_chat_history(chat.id):
                if not GLOBAL_TASK_RUNNING: break
                if not (m.video or m.document): continue
                if SERIES_KEYWORDS_REGEX.search((getattr(m.video, 'file_name', "") or "") + " " + (m.caption or "")): continue
                
                fname, fsize, uid = get_media_details(m)
                if uid: uids.add(uid)
                if fname and fsize: ckeys.add(f"{fname}-{fsize}")
                count += 1
                if count % 500 == 0: await status.edit(f"⏳ Scanned: {count}")
            
            with open(TARGET_MOVIE_INDEX_DB_FILE, "w") as f:
                json.dump({"unique_ids": list(uids), "compound_keys": list(ckeys)}, f)
            load_movie_duplicate_dbs()
            await status.edit(f"✅ Target Indexed. Unique: {len(uids)}")
        except Exception as e: await status.edit(f"Error: {e}")
        finally: GLOBAL_TASK_RUNNING = False
    app.loop.create_task(runner())

@app.on_message(filters.command("index_webseries") & filters.create(only_admin))
async def index_webseries_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING: return await message.reply("❌ Busy.")
    try: source_ref = message.text.split(" ", 1)[1].strip()
    except: return await message.reply("Usage: `/index_webseries <id>`")

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = await message.reply("⏳ Indexing Web Series...")
        try:
            chat = await resolve_chat_id(app, source_ref)
            series_list = []
            count = 0
            async for m in app.get_chat_history(chat.id):
                if not GLOBAL_TASK_RUNNING: break
                if not (m.video or m.document): continue
                
                fname, fsize, uid = get_media_details(m)
                if not uid: continue
                text = (fname or "") + " " + (m.caption or "")
                if not SERIES_KEYWORDS_REGEX.search(text): continue
                
                # Simple parsing logic
                name, season, ep_start, ep_end = "Unknown", 1, 1, None
                match = EPISODE_PACK_REGEX.search(text)
                if match: name, season, ep_start, ep_end = match.group(1).strip(), int(match.group(2)), int(match.group(3)), int(match.group(4))
                else:
                    match = EPISODE_REGEX.search(text)
                    if match: name, season, ep_start = match.group(1).strip(), int(match.group(2)), int(match.group(3))
                
                series_list.append({
                    "series_name": name, "season_num": season, "episode_num": ep_start,
                    "message_id": m.id, "chat_id": chat.id, "file_name": fname, "file_size": fsize, "file_unique_id": uid
                })
                count += 1
                if count % 500 == 0: await status.edit(f"⏳ Found: {count}")
            
            sorted_list = sorted(series_list, key=lambda x: (x['series_name'], x['season_num'], x['episode_num']))
            with open(WEBSERIES_INDEX_DB_FILE, "w") as f: json.dump(sorted_list, f, indent=2)
            await status.edit(f"✅ Indexed & Sorted {count} episodes.")
        except Exception as e: await status.edit(f"Error: {e}")
        finally: GLOBAL_TASK_RUNNING = False
    app.loop.create_task(runner())

@app.on_message(filters.command("index_full") & filters.create(only_admin))
async def index_full_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING: return await message.reply("❌ Busy.")
    try: source_ref = message.text.split(" ", 1)[1].strip()
    except: return await message.reply("Usage: `/index_full <id>`")

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = await message.reply("⏳ Full Indexing...")
        try:
            chat = await resolve_chat_id(app, source_ref)
            media_list = []
            count = 0
            async for m in app.get_chat_history(chat.id):
                if not GLOBAL_TASK_RUNNING: break
                if not (m.video or m.document): continue
                fname, fsize, uid = get_media_details(m)
                if not uid: continue
                
                media_list.append({
                    "message_id": m.id, "chat_id": chat.id, "file_name": fname, "file_size": fsize, "file_unique_id": uid
                })
                count += 1
                if count % 1000 == 0: await status.edit(f"⏳ Scanned: {count}")
            
            media_list.reverse() # Oldest to Newest
            with open(FULL_SOURCE_INDEX_DB_FILE, "w") as f: json.dump(media_list, f, indent=2)
            await status.edit(f"✅ Full Indexing Complete. Items: {count}")
        except Exception as e: await status.edit(f"Error: {e}")
        finally: GLOBAL_TASK_RUNNING = False
    app.loop.create_task(runner())

@app.on_message(filters.command("index_target_full") & filters.create(only_admin))
async def index_target_full_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING: return await message.reply("❌ Busy.")
    try: target_ref = message.text.split(" ", 1)[1].strip()
    except: return await message.reply("Usage: `/index_target_full <id>`")

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = await message.reply("⏳ Full Target Indexing...")
        try:
            chat = await resolve_chat_id(app, target_ref)
            uids = set(); ckeys = set(); count = 0
            async for m in app.get_chat_history(chat.id):
                if not GLOBAL_TASK_RUNNING: break
                if not (m.video or m.document): continue
                fname, fsize, uid = get_media_details(m)
                if uid: uids.add(uid)
                if fname and fsize: ckeys.add(f"{fname}-{fsize}")
                count += 1
                if count % 1000 == 0: await status.edit(f"⏳ Scanned: {count}")
            
            with open(FULL_TARGET_INDEX_DB_FILE, "w") as f:
                json.dump({"unique_ids": list(uids), "compound_keys": list(ckeys)}, f)
            load_full_duplicate_dbs()
            await status.edit(f"✅ Target Indexed. Unique: {len(uids)}")
        except Exception as e: await status.edit(f"Error: {e}")
        finally: GLOBAL_TASK_RUNNING = False
    app.loop.create_task(runner())

# --- DUAL CLIENT FORWARDING ---

STOP_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop", callback_data="stop_task")]])

@app.on_callback_query(filters.regex("^stop_task$"))
async def cb_stop(client, query):
    global GLOBAL_TASK_RUNNING
    if query.from_user.id != ADMIN_ID: return
    GLOBAL_TASK_RUNNING = False
    await query.answer("Stopping...")

async def run_dual_forwarding(status_msg, items, target_chat_id, db_type, limit=None):
    global GLOBAL_TASK_RUNNING, forwarded_count, mode_copy
    
    queue = asyncio.Queue()
    queued = 0
    skipped_dupes = 0
    
    # Check sets based on db_type
    if db_type == "full":
        u_set, c_set = full_fwd_unique_ids, full_target_compound_keys
    elif db_type == "webseries":
        u_set, c_set = webseries_fwd_unique_ids, webseries_target_compound_keys
    else:
        u_set, c_set = movie_fwd_unique_ids, movie_target_compound_keys

    # Fill Queue
    for item in items:
        if limit and queued >= limit: break
        
        uid = item.get("file_unique_id")
        fname = item.get("file_name")
        fsize = item.get("file_size")
        ckey = f"{fname}-{fsize}" if fname and fsize else None
        
        if (uid and uid in u_set) or (ckey and ckey in c_set):
            skipped_dupes += 1
            continue
            
        queue.put_nowait(item)
        queued += 1
    
    total_q = queue.qsize()
    await status_msg.edit(f"🚀 **Dual Engine Started**\nQueue: {total_q}\nDupes Skipped: {skipped_dupes}", reply_markup=STOP_BUTTON)
    
    async def worker_engine(client, name):
        global forwarded_count, GLOBAL_TASK_RUNNING # KEY FIX: GLOBAL not nonlocal
        while GLOBAL_TASK_RUNNING and not queue.empty():
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty: break
            
            try:
                if mode_copy: await client.copy_message(target_chat_id, item["chat_id"], item["message_id"])
                else: await client.forward_messages(target_chat_id, item["chat_id"], item["message_id"])
                
                uid = item.get("file_unique_id")
                fname = item.get("file_name")
                fsize = item.get("file_size")
                ckey = f"{fname}-{fsize}" if fname and fsize else None
                
                save_forwarded_id(uid, ckey, db_type)
                forwarded_count += 1
                await asyncio.sleep(PER_MSG_DELAY)
            except FloodWait as e:
                print(f"[{name}] FloodWait {e.value}s")
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"[{name}] Error: {e}")
            finally:
                queue.task_done()
            
            if name == "BOSS" and forwarded_count % 10 == 0:
                try: await status_msg.edit(f"🚀 Running...\nFwd: {forwarded_count}/{total_q}", reply_markup=STOP_BUTTON)
                except: pass

    t1 = asyncio.create_task(worker_engine(bot, "BOSS"))
    t2 = asyncio.create_task(worker_engine(worker, "WORKER"))
    await asyncio.gather(t1, t2)
    
    await status_msg.edit(f"✅ **Done!**\nTotal Forwarded: {forwarded_count}\nDupes: {skipped_dupes}")

@app.on_message(filters.command("forward_full") & filters.create(only_admin))
async def forward_full_cmd(_, message):
    global GLOBAL_TASK_RUNNING, forwarded_count
    if GLOBAL_TASK_RUNNING: return await message.reply("❌ Busy.")
    
    args = message.text.split(" ")
    if len(args) < 2: return await message.reply("Usage: `/forward_full <target_id> [limit]`")
    
    GLOBAL_TASK_RUNNING = True
    forwarded_count = 0
    status = await message.reply("⏳ Preparing...")
    
    try:
        tgt = await resolve_chat_id(app, args[1])
        limit = int(args[2]) if len(args) > 2 else None
        
        with open(FULL_SOURCE_INDEX_DB_FILE, "r") as f: items = json.load(f)
        load_full_duplicate_dbs()
        
        await run_dual_forwarding(status, items, tgt.id, "full", limit)
    except Exception as e: await status.edit(f"Error: {e}")
    finally: GLOBAL_TASK_RUNNING = False

@app.on_message(filters.command("forward_webseries") & filters.create(only_admin))
async def forward_webseries_cmd(_, message):
    global GLOBAL_TASK_RUNNING, forwarded_count
    if GLOBAL_TASK_RUNNING: return await message.reply("❌ Busy.")
    
    args = message.text.split(" ")
    if len(args) < 2: return await message.reply("Usage: `/forward_webseries <target_id> [limit]`")
    
    GLOBAL_TASK_RUNNING = True
    forwarded_count = 0
    status = await message.reply("⏳ Preparing...")
    
    try:
        tgt = await resolve_chat_id(app, args[1])
        limit = int(args[2]) if len(args) > 2 else None
        
        with open(WEBSERIES_INDEX_DB_FILE, "r") as f: items = json.load(f)
        load_webseries_duplicate_dbs()
        
        await run_dual_forwarding(status, items, tgt.id, "webseries", limit)
    except Exception as e: await status.edit(f"Error: {e}")
    finally: GLOBAL_TASK_RUNNING = False

@app.on_message(filters.command("start_forward") & filters.create(only_admin))
async def start_forward_cmd(_, message):
    global GLOBAL_TASK_RUNNING, forwarded_count, target_channel
    if GLOBAL_TASK_RUNNING: return await message.reply("❌ Busy.")
    if not target_channel: return await message.reply("Set target first!")
    
    GLOBAL_TASK_RUNNING = True
    forwarded_count = 0
    status = await message.reply("⏳ Preparing Movies...")
    
    try:
        tgt = await resolve_chat_id(app, target_channel)
        
        items = []
        for uid, data in movie_index["movies"].items():
            data["file_unique_id"] = uid
            data["chat_id"] = movie_index["source_channel_id"]
            items.append(data)
            
        load_movie_duplicate_dbs()
        await run_dual_forwarding(status, items, tgt.id, "movie", limit_messages)
    except Exception as e: await status.edit(f"Error: {e}")
    finally: GLOBAL_TASK_RUNNING = False

# --- Utility Commands ---
@app.on_message(filters.command("set_target") & filters.create(only_admin))
async def set_target(_, m):
    global target_channel
    target_channel = m.text.split(" ", 1)[1]
    await m.reply(f"Target set: {target_channel}")

@app.on_message(filters.command("set_limit") & filters.create(only_admin))
async def set_limit(_, m):
    global limit_messages
    limit_messages = int(m.text.split(" ", 1)[1])
    await m.reply(f"Limit set: {limit_messages}")

@app.on_message(filters.command("stop_all") & filters.create(only_admin))
async def stop_all(_, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 Stopping...")

@app.on_message(filters.command("ping") & filters.create(only_admin))
async def ping(_, m): await m.reply("✅ Pong!")

# --- Main Execution ---
async def main():
    print("📂 Loading databases...")
    load_movie_duplicate_dbs()
    load_webseries_duplicate_dbs()
    load_full_duplicate_dbs()
    load_movie_index_db()
    load_locked_content()
    
    print("🚀 Starting Dual Clients...")
    await bot.start()
    await worker.start()
    print("✅ BOSS & WORKER Online!")
    
    try: await bot.send_message(ADMIN_ID, "🚀 **Dual Engine Bot Started!**")
    except: pass
    
    await idle()
    await bot.stop()
    await worker.stop()

if __name__ == "__main__":
    bot.loop.run_until_complete(main())
