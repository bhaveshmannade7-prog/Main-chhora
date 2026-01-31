import os, re, json, asyncio, time, math
from threading import Thread
from flask import Flask
from pyrogram import Client, filters, enums, compose
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, 
    PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, 
    RPCError, UsernameInvalid, ChannelPrivate
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from collections import defaultdict

# --- DATABASE IMPORT (OPTIONAL) ---
try:
    from database import Database
    HAS_DB = True
except ImportError:
    HAS_DB = False
    print("⚠️ 'database.py' not found. MongoDB Sync features will be disabled.")

# --- CONFIGURATION (ENV VARIABLES) ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
MONGO_URL = os.getenv("MONGO_URL", "") # Required for /sync_mongo

# --- MULTI-SESSION SETUP (5 SESSIONS) ---
SESSION1 = os.getenv("SESSION1", os.getenv("SESSION_STRING"))
SESSION2 = os.getenv("SESSION2")
SESSION3 = os.getenv("SESSION3")
SESSION4 = os.getenv("SESSION4")
SESSION5 = os.getenv("SESSION5")

if not SESSION1:
    print("❌ CRITICAL ERROR: SESSION1 is missing! Bot cannot start.")
    exit(1)

# --- WEB SERVER (RENDER VS MOBILE) ---
app_web = Flask(__name__)

@app_web.route('/')
def home():
    return "✅ Ultra Bot V6 (Production) is Running!"

def run_web_server():
    port = int(os.getenv("PORT", 8080))
    app_web.run(host="0.0.0.0", port=port)

def start_web_server():
    # Only start web server if on Cloud (Render)
    if os.getenv("PORT") or os.getenv("RENDER"):
        print("🌍 Server Environment Detected: Starting Web Server...")
        t = Thread(target=run_web_server)
        t.start()
    else:
        print("📱 Mobile/Termux Environment: Web Server Disabled (Saving RAM).")

# --- CLIENT INITIALIZATION ---
# Using in_memory=True for speed and RAM optimization on mobile
app = Client("adv_bot_1", api_id=API_ID, api_hash=API_HASH, session_string=SESSION1, in_memory=True, ipv6=False)
app2 = Client("adv_bot_2", api_id=API_ID, api_hash=API_HASH, session_string=SESSION2 if SESSION2 else SESSION1, in_memory=True, ipv6=False)
app3 = Client("adv_bot_3", api_id=API_ID, api_hash=API_HASH, session_string=SESSION3 if SESSION3 else SESSION1, in_memory=True, ipv6=False)
app4 = Client("adv_bot_4", api_id=API_ID, api_hash=API_HASH, session_string=SESSION4 if SESSION4 else SESSION1, in_memory=True, ipv6=False)
app5 = Client("adv_bot_5", api_id=API_ID, api_hash=API_HASH, session_string=SESSION5 if SESSION5 else SESSION1, in_memory=True, ipv6=False)

ALL_CLIENTS = [app]
if SESSION2: ALL_CLIENTS.append(app2)
if SESSION3: ALL_CLIENTS.append(app3)
if SESSION4: ALL_CLIENTS.append(app4)
if SESSION5: ALL_CLIENTS.append(app5)

# --- GLOBAL SETTINGS ---
GLOBAL_TASK_RUNNING = False
PER_MSG_DELAY = 0.2
BATCH_SIZE = 250
BREAK_TIME = 35 # Safety break

# --- FILES ---
DB_FILES = {
    "movie_source": "db_movie_source.json", # For /forward_movie
    "movie_target": "db_movie_target.json",
    "full_source": "db_full_source.json",   # For /index_full (Library Cleaning)
    "full_target": "db_full_target.json",
    "history": "history_ids.txt",
    "rules_cleaning": "rules_cleaning.json",
    "rules_quality": "rules_quality.json",
    "pending_actions": "pending_actions.json",
    "mongo_orphans": "mongo_orphans.json"
}

# --- MEMORY CACHE ---
target_cache = {
    "unique_ids": set(),
    "name_size": set()
}

# --- DEFAULT RULES (Auto-Generated) ---
DEFAULT_QUALITY_RULES = {
  "bad_quality": ["cam", "camrip", "hdcam", "ts", "telesync", "tc", "scr", "screener", "line audio", "pre-dvd", "hd-rip"],
  "good_quality": ["2160p", "4k", "1080p", "720p", "web-dl", "bluray", "dvdrip", "hdrip"],
  "extensions": [".mkv", ".mp4", ".avi", ".webm"]
}

DEFAULT_CLEANING_RULES = {
  "remove": ["@old_channel_username", "t.me/spam_link", "Join Now"],
  "replace": {
      "@source_channel": "@my_new_channel",
      "t.me/source": "t.me/my_link"
  },
  "locked_users": ["@my_admin_user", "@my_main_channel"],
  "locked_links": ["t.me/my_permanent_link"]
}

# --- HELPER FUNCTIONS ---

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def load_json(path, default=None):
    if os.path.exists(path):
        try:
            with open(path, "r") as f: return json.load(f)
        except: return default if default else []
    return default if default else []

def save_json(path, data):
    with open(path, "w") as f: json.dump(data, f, indent=2)

# Init Rules
if not os.path.exists(DB_FILES["rules_cleaning"]): save_json(DB_FILES["rules_cleaning"], DEFAULT_CLEANING_RULES)
if not os.path.exists(DB_FILES["rules_quality"]): save_json(DB_FILES["rules_quality"], DEFAULT_QUALITY_RULES)

def get_media_details(m):
    """Extracts File Name, Size, Unique ID, and Caption."""
    media = m.video or m.document
    if not media: return None, 0, None, None
    
    # Strict Video Check for Documents
    if m.document:
        mime = getattr(m.document, "mime_type", "")
        if not mime or "video" not in mime:
            fname = getattr(m.document, "file_name", "").lower()
            if not fname.endswith(('.mkv', '.mp4', '.avi', '.webm', '.mov')): return None, 0, None, None

    fname = getattr(media, 'file_name', "Unknown")
    fsize = getattr(media, 'file_size', 0)
    funique = getattr(media, 'file_unique_id', None)
    caption = m.caption or "" 
    
    return fname, fsize, funique, caption

def normalize_title(title):
    """Normalize for smart grouping (ignores Year, Quality, Special Chars)."""
    title = title.lower()
    q_rules = load_json(DB_FILES["rules_quality"], DEFAULT_QUALITY_RULES)
    
    # Remove all quality keywords
    all_keywords = q_rules["bad_quality"] + q_rules["good_quality"] + ["10bit", "hevc", "x265", "x264", "hindi", "dual audio"]
    for k in all_keywords:
        title = title.replace(k, "")
    
    # Remove Years (1950-2029)
    title = re.sub(r'(19|20)\d{2}', '', title)
    # Keep only alphanumeric
    title = re.sub(r'[^a-z0-9]', '', title)
    return title

async def resolve_chat_id(client, ref):
    ref_str = str(ref).strip()
    try:
        if ref_str.lstrip('-').isdigit(): return await client.get_chat(int(ref_str))
        if "t.me/+" in ref_str or "joinchat" in ref_str: return await client.join_chat(ref_str)
        return await client.get_chat(ref_str)
    except: raise ValueError("Could not resolve Chat ID/Link.")

def load_target_cache(db_file):
    """Loads Target Cache to prevent duplicates."""
    global target_cache
    target_cache["unique_ids"].clear()
    target_cache["name_size"].clear()
    
    if os.path.exists(DB_FILES["history"]):
        with open(DB_FILES["history"], "r") as f:
            for line in f: target_cache["unique_ids"].add(line.strip())

    data = load_json(db_file, {})
    target_cache["unique_ids"].update(data.get("unique_ids", []))
    target_cache["name_size"].update(data.get("compound_keys", []))

def save_history(unique_id, name, size):
    if unique_id:
        target_cache["unique_ids"].add(unique_id)
        with open(DB_FILES["history"], "a") as f: f.write(f"{unique_id}\n")
    if name and size:
        target_cache["name_size"].add(f"{name}-{size}")

# ==============================================================================
# 🚀 ENGINE 1: INDEXING (Includes Index, Index Full, Target Index)
# ==============================================================================
async def indexing_engine(client, message, chat_ref, db_file, mode="all"):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply(f"🚀 **Indexing Started**\nTarget: `{chat_ref}`\nMode: `{mode}`\n\n_Scanning..._")
    
    try:
        chat = await resolve_chat_id(client, chat_ref)
        data_list = []
        unique_ids_set = set()
        name_size_set = set()
        
        count = 0
        found = 0
        
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING: break
            
            # Use extended media extractor
            fname, fsize, funique, caption = get_media_details(m)
            if not funique: continue

            # For Target DB (Only Cache Keys)
            if "target" in db_file:
                unique_ids_set.add(funique)
                if fname and fsize: name_size_set.add(f"{fname}-{fsize}")
            # For Source DB (Full Data for Forwarding/Cleaning)
            else:
                data_list.append({
                    "msg_id": m.id,
                    "chat_id": chat.id,
                    "unique_id": funique,
                    "name": fname,
                    "size": fsize,
                    "caption": caption # Stored for cleaning features
                })
            
            found += 1
            count += 1
            if count % 1000 == 0:
                try: await status.edit(f"⚡ Indexing `{chat.title}`: {count} scanned...")
                except: pass

        if "target" in db_file:
            with open(db_file, "w") as f:
                json.dump({"unique_ids": list(unique_ids_set), "compound_keys": list(name_size_set)}, f)
            await status.edit(f"✅ **Target Index Done!**\nFound: {len(unique_ids_set)} unique files.")
        else:
            data_list.reverse() # Oldest to Newest
            with open(db_file, "w") as f: json.dump(data_list, f, indent=2)
            await status.edit(f"✅ **Source Index Done!**\nFound: {len(data_list)} files.\nSaved to `{db_file}`.")

    except Exception as e:
        await status.edit(f"❌ Index Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

# ==============================================================================
# 🚀 ENGINE 2: FORWARDING (5-Session Parallel + Partitioning)
# ==============================================================================
async def forwarding_engine(message, source_db, target_db, destination_ref, limit=None, mode_copy=True):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("⚙️ **Preparing 5-Core Forwarding...**")
    
    if not os.path.exists(source_db): return await status.edit("❌ Source DB not found!")
    
    source_data = load_json(source_db)
    load_target_cache(target_db)
    
    try:
        dest_chat = await resolve_chat_id(app, destination_ref)
        dest_id = dest_chat.id
    except Exception as e: return await status.edit(f"❌ Destination Error: {e}")
    
    # Filter Duplicates
    final_list = []
    skipped = 0
    for item in source_data:
        key = f"{item['name']}-{item['size']}"
        if (item['unique_id'] in target_cache["unique_ids"]) or (key in target_cache["name_size"]):
            skipped += 1
            continue
        final_list.append(item)
    
    if limit and int(limit) > 0: final_list = final_list[:int(limit)]
    if not final_list: return await status.edit("✅ No new files to forward.")

    # PARTITIONING LOGIC
    active_sessions = [c for c in ALL_CLIENTS if c.is_connected]
    chunk_size = math.ceil(len(final_list) / len(active_sessions))
    chunks = [final_list[i:i + chunk_size] for i in range(0, len(final_list), chunk_size)]
    while len(chunks) < len(active_sessions): chunks.append([])

    await status.edit(f"🚀 **Forwarding Started!**\nFiles: {len(final_list)}\nSkipped: {skipped}\nSessions: {len(active_sessions)}")
    
    progress = {"sent": 0}

    async def worker(client, tasks, name):
        local_batch = 0
        for item in tasks:
            if not GLOBAL_TASK_RUNNING: break
            try:
                if mode_copy: await client.copy_message(dest_id, item['chat_id'], item['msg_id'])
                else: await client.forward_messages(dest_id, item['chat_id'], item['msg_id'])
                
                save_history(item['unique_id'], item['name'], item['size'])
                progress["sent"] += 1
                local_batch += 1
                
                if progress["sent"] % 50 == 0:
                    try: await status.edit(f"🔄 **Forwarding...** {progress['sent']} / {len(final_list)}")
                    except: pass
                
                if local_batch >= BATCH_SIZE:
                    await asyncio.sleep(BREAK_TIME)
                    local_batch = 0
                else:
                    await asyncio.sleep(PER_MSG_DELAY)
            
            except FloodWait as e:
                print(f"[{name}] FloodWait {e.value}s")
                await asyncio.sleep(e.value + 5)
            except Exception as e: print(f"[{name}] Error: {e}")

    # Launch Tasks
    tasks = []
    for i, session in enumerate(active_sessions):
        if i < len(chunks):
            tasks.append(worker(session, chunks[i], f"Session-{i+1}"))
    
    await asyncio.gather(*tasks)
    GLOBAL_TASK_RUNNING = False
    await status.edit("✅ **Forwarding Complete!**")

# ==============================================================================
# 🚀 ENGINE 3: ANALYSIS (Edits & Library Cleaning - DRY RUN)
# ==============================================================================
async def analyze_cleaning_engine(message, mode="library"):
    """
    mode='library': Checks Quality + Duplicates
    mode='edits': Checks Captions for Links/Usernames
    """
    status = await message.reply("🕵️ **Running Analysis (Dry Run)...**")
    
    data = load_json(DB_FILES["full_source"]) # Must use full source
    if not data: return await status.edit("❌ Full Index Empty! Run `/index_full`.")
    
    actions = []
    
    # --- MODE: EDIT CAPTIONS ---
    if mode == "edits":
        rules = load_json(DB_FILES["rules_cleaning"], DEFAULT_CLEANING_RULES)
        stats = 0
        
        for item in data:
            orig = item.get("caption", "")
            if not orig: continue
            
            new_cap = orig
            modified = False
            
            # Remove Rules
            for rem in rules.get("remove", []):
                if rem in new_cap and rem not in rules["locked_users"] and rem not in rules["locked_links"]:
                    new_cap = new_cap.replace(rem, "")
                    modified = True
            
            # Replace Rules
            for old, new in rules.get("replace", {}).items():
                if old in new_cap and old not in rules["locked_users"]:
                    new_cap = new_cap.replace(old, new)
                    modified = True
            
            if modified and new_cap.strip() != orig.strip():
                actions.append({
                    "type": "edit",
                    "chat_id": item["chat_id"],
                    "msg_id": item["msg_id"],
                    "new_caption": new_cap.strip()
                })
                stats += 1
        
        msg = f"📝 **Caption Edit Analysis**\nFound `{stats}` messages to edit.\nType `/confirm_action` to execute."

    # --- MODE: LIBRARY CLEANING (Quality + Duplicates) ---
    elif mode == "library":
        q_rules = load_json(DB_FILES["rules_quality"], DEFAULT_QUALITY_RULES)
        
        # 1. Group by Normalized Name
        movies = defaultdict(list)
        for item in data:
            norm = normalize_title(item["name"])
            if norm: movies[norm].append(item)
        
        stats = {"dupes": 0, "bad": 0, "kept": 0}
        
        for norm_name, items in movies.items():
            # 2. Sub-group by Quality
            q_groups = defaultdict(list)
            has_good = False
            
            for item in items:
                name_lower = item["name"].lower()
                q_type = "unknown"
                if any(k in name_lower for k in q_rules["good_quality"]):
                    q_type = "good"
                    has_good = True
                elif any(k in name_lower for k in q_rules["bad_quality"]):
                    q_type = "bad"
                q_groups[q_type].append(item)
            
            # 3. Mark BAD for delete IF GOOD exists
            if has_good:
                for bad_item in q_groups["bad"]:
                    actions.append({"type": "delete", "chat_id": bad_item["chat_id"], "msg_id": bad_item["msg_id"]})
                    stats["bad"] += 1
                q_groups["bad"] = [] # Remove from memory
            
            # 4. Duplicate Cleaner (Keep Largest Size per Quality)
            for q_type, q_items in q_groups.items():
                if len(q_items) > 1:
                    q_items.sort(key=lambda x: x["size"], reverse=True) # Largest first
                    keep = q_items[0]
                    trash = q_items[1:]
                    
                    for t in trash:
                        actions.append({"type": "delete", "chat_id": t["chat_id"], "msg_id": t["msg_id"]})
                        stats["dupes"] += 1
                    stats["kept"] += 1
                elif len(q_items) == 1:
                    stats["kept"] += 1

        msg = (f"📊 **Library Analysis**\n"
               f"🗑️ Duplicates to Delete: `{stats['dupes']}`\n"
               f"💩 Bad Quality to Delete: `{stats['bad']}`\n"
               f"✅ Files to Keep: `{stats['kept']}`\n"
               f"Type `/confirm_action` to execute.")

    # Save Pending Actions
    save_json(DB_FILES["pending_actions"], actions)
    await status.edit(msg)

# ==============================================================================
# 🚀 ENGINE 4: EXECUTION (5-Session Parallel Delete/Edit)
# ==============================================================================
async def execution_engine(message):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("🔥 **Executing Actions (5-Core)...**")
    actions = load_json(DB_FILES["pending_actions"])
    
    if not actions: return await status.edit("❌ No pending actions. Run `/analyze_*` first.")
    
    # Split actions
    active_sessions = [c for c in ALL_CLIENTS if c.is_connected]
    chunk_size = math.ceil(len(actions) / len(active_sessions))
    chunks = [actions[i:i + chunk_size] for i in range(0, len(actions), chunk_size)]
    while len(chunks) < len(active_sessions): chunks.append([])
    
    progress = {"done": 0, "total": len(actions)}
    
    async def worker(client, tasks, name):
        local_cnt = 0
        delete_ids = []
        last_chat_id = None
        
        for task in tasks:
            if not GLOBAL_TASK_RUNNING: break
            try:
                # DELETE BATCHING
                if task["type"] == "delete":
                    if last_chat_id and last_chat_id != task["chat_id"] and delete_ids:
                        await client.delete_messages(last_chat_id, delete_ids)
                        delete_ids = []
                    
                    delete_ids.append(task["msg_id"])
                    last_chat_id = task["chat_id"]
                    progress["done"] += 1
                    
                    if len(delete_ids) >= 50:
                        await client.delete_messages(last_chat_id, delete_ids)
                        delete_ids = []
                        local_cnt += 1
                        await asyncio.sleep(1)

                # EDIT (One by One)
                elif task["type"] == "edit":
                    await client.edit_message_caption(
                        chat_id=task["chat_id"], 
                        message_id=task["msg_id"], 
                        caption=task["new_caption"]
                    )
                    progress["done"] += 1
                    local_cnt += 1
                
                # UI Update
                if progress["done"] % 50 == 0:
                    try: await status.edit(f"⚙️ **Processing...** {progress['done']} / {progress['total']}")
                    except: pass
                
                # Safety Break
                if local_cnt >= (BATCH_SIZE / 5):
                    await asyncio.sleep(BREAK_TIME)
                    local_cnt = 0
            
            except FloodWait as e:
                print(f"[{name}] FloodWait {e.value}s")
                await asyncio.sleep(e.value + 3)
            except Exception as e: print(f"[{name}] Error: {e}")
        
        # Flush remaining deletes
        if delete_ids and last_chat_id:
            try: await client.delete_messages(last_chat_id, delete_ids)
            except: pass

    # Run Parallel
    tasks = []
    for i, session in enumerate(active_sessions):
        if i < len(chunks):
            tasks.append(worker(session, chunks[i], f"S{i+1}"))
            
    await asyncio.gather(*tasks)
    GLOBAL_TASK_RUNNING = False
    save_json(DB_FILES["pending_actions"], []) # Clear
    await status.edit("✅ **Execution Complete!**")

# ==============================================================================
# 🚀 ENGINE 5: MONGO DB SYNC
# ==============================================================================
async def mongo_sync_engine(message):
    if not HAS_DB or not MONGO_URL: return await message.reply("❌ DB Module Missing or MONGO_URL not set.")
    
    status = await message.reply("🔄 **Syncing with MongoDB...**")
    
    db = Database(MONGO_URL)
    if not await db.init_db(): return await status.edit("❌ DB Connection Failed.")
    
    # Load Source of Truth (Channel JSON)
    if not os.path.exists(DB_FILES["full_source"]): return await status.edit("❌ Index Full first!")
    channel_data = load_json(DB_FILES["full_source"])
    channel_uniques = set(i["unique_id"] for i in channel_data)
    
    # Get Mongo Data
    await status.edit("📥 **Fetching Mongo Data...**")
    mongo_movies = await db.get_all_movies_for_neon_sync()
    if not mongo_movies: return await status.edit("❌ No data in Mongo.")
    
    orphans = []
    for m in mongo_movies:
        if m.get("file_unique_id") not in channel_uniques:
            orphans.append(m.get("imdb_id"))
            
    if not orphans: return await status.edit("✅ **Sync Perfect!** No orphaned files.")
    
    save_json(DB_FILES["mongo_orphans"], orphans)
    btn = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Delete Orphans", callback_data="conf_mongo")]])
    await message.reply(f"⚠️ **Mismatch!**\nOrphans in DB: `{len(orphans)}`\nDelete from DB?", reply_markup=btn)

@app.on_callback_query(filters.regex("conf_mongo"))
async def cb_mongo(c, q):
    orphans = load_json(DB_FILES["mongo_orphans"])
    await q.message.edit(f"🗑️ **Deleting {len(orphans)} items from Mongo...**")
    db = Database(MONGO_URL)
    await db.init_db()
    for imdb_id in orphans: await db.remove_movie_by_imdb(imdb_id)
    await q.message.edit("✅ **DB Sync Complete!**")

# --- COMMANDS ---

@app.on_message(filters.command("start") & filters.create(only_admin))
async def start(_, m):
    await m.reply(
        "🤖 **Ultra Bot V6 (Complete Manager)**\n\n"
        "**1. Forwarding**\n"
        "`/index <channel>` - Index Movies\n"
        "`/index_target <channel>` - Anti-Duplicate Index\n"
        "`/forward_movie <target>` - Start Forwarding\n\n"
        "**2. Library Cleaning**\n"
        "`/index_full <channel>` - Index Everything (Required)\n"
        "`/analyze_library` - Check Dupes & Quality\n"
        "`/analyze_edits` - Check Links/Captions\n"
        "`/confirm_action` - EXECUTE PENDING ACTIONS\n\n"
        "**3. Database**\n"
        "`/sync_mongo` - Clean DB Orphans\n"
        "`/set_lock <value>` - Protect User/Link\n"
        "`/stop` - Stop Tasks"
    )

@app.on_message(filters.command("index") & filters.create(only_admin))
async def c_idx(c, m): await indexing_engine(c, m, m.command[1], DB_FILES["movie_source"], "movie")

@app.on_message(filters.command("index_target") & filters.create(only_admin))
async def c_idxt(c, m): await indexing_engine(c, m, m.command[1], DB_FILES["movie_target"], "target")

@app.on_message(filters.command("index_full") & filters.create(only_admin))
async def c_idxf(c, m): await indexing_engine(c, m, m.command[1], DB_FILES["full_source"], "full")

@app.on_message(filters.command("forward_movie") & filters.create(only_admin))
async def c_fwd(c, m):
    limit = m.command[2] if len(m.command) > 2 else None
    await forwarding_engine(m, DB_FILES["movie_source"], DB_FILES["movie_target"], m.command[1], limit)

@app.on_message(filters.command("analyze_library") & filters.create(only_admin))
async def c_anal_lib(c, m): await analyze_cleaning_engine(m, "library")

@app.on_message(filters.command("analyze_edits") & filters.create(only_admin))
async def c_anal_edit(c, m): await analyze_cleaning_engine(m, "edits")

@app.on_message(filters.command("confirm_action") & filters.create(only_admin))
async def c_conf(c, m): await execution_engine(m)

@app.on_message(filters.command("sync_mongo") & filters.create(only_admin))
async def c_sync(c, m): await mongo_sync_engine(m)

@app.on_message(filters.command("stop") & filters.create(only_admin))
async def c_stop(c, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 **Stopping All Tasks...**")

@app.on_message(filters.command("set_lock") & filters.create(only_admin))
async def c_lock(c, m):
    if len(m.command) < 2: return
    val = m.command[1]
    rules = load_json(DB_FILES["rules_cleaning"], DEFAULT_CLEANING_RULES)
    if "@" in val: rules["locked_users"].append(val)
    else: rules["locked_links"].append(val)
    save_json(DB_FILES["rules_cleaning"], rules)
    await m.reply(f"🔒 **Locked:** `{val}`")

# --- MAIN ---
if __name__ == "__main__":
    print("🤖 Ultra Bot V6 Starting...")
    start_web_server() 
    compose(ALL_CLIENTS)
