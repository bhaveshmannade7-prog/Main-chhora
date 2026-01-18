import os, re, json, asyncio, time
from threading import Thread, Event
from flask import Flask
from pyrogram import Client, filters, enums
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, 
    PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, 
    RPCError, UsernameInvalid, ChannelPrivate
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- CONFIGURATION (ENV VARIABLES) ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Multi-Session Setup
SESSION1 = os.getenv("SESSION_STRING")       # Main Admin / Indexer
SESSION2 = os.getenv("SESSION_STRING_2")     # Worker 1
SESSION3 = os.getenv("SESSION_STRING_3")     # Worker 2

# Deployment Mode Detection
IS_RENDER = os.getenv("RENDER", "False").lower() in ("true", "1", "yes")

# --- WEB SERVER (RENDER ONLY) ---
app_web = Flask(__name__)

@app_web.route('/')
def home():
    return "✅ Bot is Running Successfully! Port is Open."

def run_web_server():
    port = int(os.getenv("PORT", 8080))
    app_web.run(host="0.0.0.0", port=port)

def start_web_server():
    if IS_RENDER:
        t = Thread(target=run_web_server, daemon=True)
        t.start()
        print("🌍 Web Server Started (Render Mode)")
    else:
        print("📱 Phone/Termux Mode: Web Server Disabled to save battery/ports.")

# --- ADVANCED CLIENT SETUP (MULTI-CLIENT) ---
# Common args for all clients
client_args = {
    "api_id": API_ID,
    "api_hash": API_HASH,
    "in_memory": True,
    "ipv6": False,
    "no_updates": True  # Workers don't need incoming updates, saves bandwidth
}

# Main Client (Handles Commands & Indexing)
app1 = Client("worker_1", session_string=SESSION1, **{**client_args, "no_updates": False})

# Secondary Clients (Pure Forwarders)
# If sessions aren't provided, they default to None and won't be used (Graceful fallback)
app2 = Client("worker_2", session_string=SESSION2, **client_args) if SESSION2 else None
app3 = Client("worker_3", session_string=SESSION3, **client_args) if SESSION3 else None

# List of active clients for forwarding
WORKER_CLIENTS = [c for c in [app1, app2, app3] if c is not None]

# --- GLOBAL SETTINGS (OPTIMIZED) ---
GLOBAL_TASK_RUNNING = False
PER_MSG_DELAY = 0.2        # Base delay
BATCH_SIZE = 250           
BREAK_TIME = 30            

# --- DATABASE FILES ---
DB_FILES = {
    "movie_source": "db_movie_source.json",
    "movie_target": "db_movie_target.json",
    "full_source": "db_full_source.json",
    "full_target": "db_full_target.json",
    "history": "history_ids.txt"
}

# --- MEMORY CACHE ---
target_cache = {
    "unique_ids": set(),
    "name_size": set()
}

# --- SHARED PROGRESS TRACKER ---
class ProgressTracker:
    def __init__(self):
        self.total = 0
        self.success = 0
        self.skipped = 0
        self.failed = 0
        self.lock = asyncio.Lock()

    async def update(self, status="success"):
        async with self.lock:
            if status == "success": self.success += 1
            elif status == "skipped": self.skipped += 1
            elif status == "failed": self.failed += 1

# --- REGEX PATTERNS ---
BAD_QUALITY_REGEX = re.compile(r"\b(cam|camrip|hdcam|ts|telesync|tc|pre-dvdrip|scr|screener|bad audio)\b", re.IGNORECASE)

# --- HELPER FUNCTIONS ---

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def get_media_details(m):
    media = m.video or m.document
    if not media:
        return None, 0, None
    
    if m.document:
        mime = getattr(m.document, "mime_type", "")
        if not mime or "video" not in mime:
            fname = getattr(m.document, "file_name", "").lower()
            if not fname.endswith(('.mkv', '.mp4', '.avi', '.webm', '.mov')):
                return None, 0, None

    return getattr(media, 'file_name', "Unknown"), getattr(media, 'file_size', 0), getattr(media, 'file_unique_id', None)

async def resolve_chat_id(client, ref):
    ref_str = str(ref).strip()
    try:
        if ref_str.lstrip('-').isdigit():
            return await client.get_chat(int(ref_str))
    except: pass

    if "t.me/+" in ref_str or "joinchat" in ref_str:
        try:
            return await client.join_chat(ref_str)
        except UserAlreadyParticipant:
            pass
        except Exception as e:
            raise ValueError(f"❌ Invite Link Error: {e}")

    try:
        return await client.get_chat(ref_str)
    except Exception as e:
        raise ValueError(f"❌ Chat resolve fail: {e}")

def load_target_cache(db_file):
    global target_cache
    target_cache["unique_ids"].clear()
    target_cache["name_size"].clear()
    
    if os.path.exists(DB_FILES["history"]):
        with open(DB_FILES["history"], "r") as f:
            for line in f:
                target_cache["unique_ids"].add(line.strip())

    if os.path.exists(db_file):
        try:
            with open(db_file, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    target_cache["unique_ids"].update(data.get("unique_ids", []))
                    target_cache["name_size"].update(data.get("compound_keys", []))
        except Exception as e:
            print(f"Cache Load Warning: {e}")

    print(f"✅ Cache Loaded: {len(target_cache['unique_ids'])} UIDs")

def save_history(unique_id, name, size):
    if unique_id:
        target_cache["unique_ids"].add(unique_id)
        with open(DB_FILES["history"], "a") as f:
            f.write(f"{unique_id}\n")
    if name and size:
        target_cache["name_size"].add(f"{name}-{size}")

def split_list(data, n):
    """Splits a list into n chunks deterministically."""
    k, m = divmod(len(data), n)
    return [data[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)]

# --- WORKER ENGINE ---

async def worker_forwarding_task(client_obj, data_chunk, target_chat_id, progress, mode_copy):
    """
    Isolated worker process for a single client.
    Handles its own delays and FloodWait without affecting others.
    """
    batch_counter = 0
    
    # Resolve target chat specifically for this client (needed for access hash)
    try:
        dest_chat = await client_obj.get_chat(target_chat_id)
    except Exception:
        # If get_chat fails, try to join if it's not private
        # For simplicity in workers, we assume app1 resolved access or its a public chat/id
        dest_chat = await client_obj.get_chat(target_chat_id)

    for item in data_chunk:
        if not GLOBAL_TASK_RUNNING:
            break
            
        try:
            if mode_copy:
                await client_obj.copy_message(dest_chat.id, item['chat_id'], item['msg_id'])
            else:
                await client_obj.forward_messages(dest_chat.id, item['chat_id'], item['msg_id'])
            
            # Save History & Update Progress
            save_history(item.get("unique_id"), item.get("name"), item.get("size"))
            await progress.update("success")
            
            batch_counter += 1
            
            # --- SAFETY BREAK LOGIC ---
            if batch_counter >= BATCH_SIZE:
                # Local break for this worker only
                await asyncio.sleep(BREAK_TIME)
                batch_counter = 0
            else:
                await asyncio.sleep(PER_MSG_DELAY)

        except FloodWait as e:
            # Silent Wait - Do not update status, just wait
            # Other workers will continue running!
            await asyncio.sleep(e.value + 5)
        except (MessageIdInvalid, MessageAuthorRequired):
            await progress.update("failed")
        except Exception as e:
            print(f"Worker Error: {e}")
            await progress.update("failed")

# --- MAIN ENGINES ---

async def indexing_engine(client, message, chat_ref, db_file, mode="all"):
    """
    Standard Indexing Engine (Runs on Main Client app1)
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply(f"🚀 **Indexing Started**\nTarget: `{chat_ref}`\nMode: `{mode}`")
    
    try:
        chat = await resolve_chat_id(client, chat_ref)
        data_list = []
        unique_ids_set = set()
        name_size_set = set()
        count = 0
        found = 0
        
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING:
                await status.edit("🛑 Stopped.")
                return

            if not (m.video or m.document): continue
            
            fname, fsize, uid = get_media_details(m)
            if not uid: continue

            if "target" in db_file:
                unique_ids_set.add(uid)
                if fname and fsize: name_size_set.add(f"{fname}-{fsize}")
            else:
                data_list.append({
                    "msg_id": m.id, "chat_id": chat.id,
                    "unique_id": uid, "name": fname, "size": fsize
                })
            
            found += 1
            count += 1
            
            if count % 500 == 0:
                try: await status.edit(f"⚡ **Scanning**\nChecked: {count}\nFound: {found}")
                except: pass

        if "target" in db_file:
            final_data = {"unique_ids": list(unique_ids_set), "compound_keys": list(name_size_set)}
            with open(db_file, "w") as f: json.dump(final_data, f)
            msg = f"✅ **Target Indexed**\nUIDs: `{len(unique_ids_set)}`"
        else:
            data_list.reverse()
            with open(db_file, "w") as f: json.dump(data_list, f, indent=2)
            msg = f"✅ **Source Indexed**\nFiles: `{len(data_list)}`"

        await status.edit(msg)

    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

async def multi_session_forwarding_manager(message, source_db, target_db, destination_ref, limit=None, mode_copy=True):
    """
    🚀 Multi-Session Manager
    - Splits data into 3 chunks.
    - Assigns chunks to app1, app2, app3.
    - Monitors progress centrally.
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("⚙️ **Initializing Multi-Session Engine...**")
    
    # 1. Load Data
    if not os.path.exists(source_db): return await status.edit("❌ Source missing.")
    try:
        with open(source_db, "r") as f: source_data = json.load(f)
    except: return await status.edit("❌ DB Corrupt.")

    load_target_cache(target_db)
    
    # 2. Resolve Destination (Main Client)
    try:
        dest_chat = await resolve_chat_id(app1, destination_ref)
    except Exception as e:
        return await status.edit(f"❌ Dest Error: {e}")

    # 3. Filter Duplicates
    await status.edit("🔍 **Filtering Duplicates...**")
    clean_list = []
    skipped_init = 0
    
    for item in source_data:
        key = f"{item.get('name')}-{item.get('size')}"
        if (item.get('unique_id') in target_cache["unique_ids"]) or (key in target_cache["name_size"]):
            skipped_init += 1
            continue
        clean_list.append(item)
    
    if limit and int(limit) > 0:
        clean_list = clean_list[:int(limit)]
        
    if not clean_list:
        return await status.edit(f"✅ Nothing to forward.\nSkipped: {skipped_init}")

    # 4. Split Data for Workers
    active_workers = WORKER_CLIENTS 
    num_workers = len(active_workers)
    
    if num_workers == 0:
        return await status.edit("❌ No clients initialized!")

    chunks = split_list(clean_list, num_workers)
    progress_tracker = ProgressTracker()
    progress_tracker.total = len(clean_list)
    progress_tracker.skipped = skipped_init

    await status.edit(
        f"🚀 **Starting Multi-Session Forwarding**\n\n"
        f"👥 Active Accounts: `{num_workers}`\n"
        f"📂 Total Files: `{len(clean_list)}`\n"
        f"✂️ Split Strategy: `{len(clean_list)} / {num_workers}` per account\n"
        f"🛡️ Duplicate Check: Active"
    )

    # 5. Launch Workers
    tasks = []
    for i, client_obj in enumerate(active_workers):
        if chunks[i]: # Only start if chunk has data
            task = asyncio.create_task(
                worker_forwarding_task(client_obj, chunks[i], dest_chat.id, progress_tracker, mode_copy)
            )
            tasks.append(task)

    # 6. Monitor Loop (Only main thread updates Status)
    while any(not t.done() for t in tasks):
        if not GLOBAL_TASK_RUNNING:
            for t in tasks: t.cancel()
            break
            
        await asyncio.sleep(8) # Update status every 8s to avoid rate limits
        
        try:
            percent = (progress_tracker.success / progress_tracker.total) * 100 if progress_tracker.total > 0 else 0
            await status.edit(
                f"🔄 **Multi-Forwarding...**\n\n"
                f"✅ Success: `{progress_tracker.success}`\n"
                f"⚠️ Failed: `{progress_tracker.failed}`\n"
                f"📊 Progress: `{percent:.1f}%`\n\n"
                f"_Running on {num_workers} sessions..._"
            )
        except FloodWait: 
            pass # Don't crash monitor on status update floodwait
        except Exception: 
            pass

    await status.edit(
        f"🎉 **Task Completed!**\n\n"
        f"✅ Total Forwarded: `{progress_tracker.success}`\n"
        f"🗑️ Total Skipped: `{progress_tracker.skipped}`\n"
        f"👥 Workers Used: `{num_workers}`"
    )
    GLOBAL_TASK_RUNNING = False

# --- COMMANDS ---

@app1.on_message(filters.command("start") & filters.create(only_admin))
async def start_msg(_, m):
    txt = (
        "🤖 **Multi-Session Movie Bot (Termux/Render)**\n\n"
        "**Index:** `/index`, `/index_full`, `/index_target`\n"
        "**Forward:** `/forward_movie`, `/forward_full`\n"
        "**Control:** `/stop`, `/stats`\n\n"
        f"Running on: `{'Render (Web Enabled)' if IS_RENDER else 'Phone (Optimized)'}`"
    )
    await m.reply(txt)

@app1.on_message(filters.command("stats") & filters.create(only_admin))
async def stats_cmd(_, m):
    mov = len(json.load(open(DB_FILES["movie_source"]))) if os.path.exists(DB_FILES["movie_source"]) else 0
    ids = len(target_cache["unique_ids"])
    workers = len(WORKER_CLIENTS)
    
    txt = (
        "📊 **System Stats**\n\n"
        f"⚡ Running: `{GLOBAL_TASK_RUNNING}`\n"
        f"👥 Active Sessions: `{workers}`\n"
        f"📱 Mode: `{'Render' if IS_RENDER else 'Termux'}`\n"
        f"📂 Movie Source: `{mov}`\n"
        f"🛡️ Cache Size: `{ids}`"
    )
    await m.reply(txt)

@app1.on_message(filters.command("stop") & filters.create(only_admin))
async def stop_cmd(_, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 **Stopping All Tasks...**\nWorkers will finish current batch and stop.")

# --- INDEX HANDLERS ---

@app1.on_message(filters.command("index") & filters.create(only_admin))
async def cmd_idx_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["movie_source"], mode="movie")

@app1.on_message(filters.command("index_target") & filters.create(only_admin))
async def cmd_idx_tgt_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["movie_target"], mode="target")

@app1.on_message(filters.command("index_full") & filters.create(only_admin))
async def cmd_idx_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_full @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["full_source"], mode="all")

@app1.on_message(filters.command("index_target_full") & filters.create(only_admin))
async def cmd_idx_tgt_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target_full @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["full_target"], mode="target")

# --- FORWARD HANDLERS (ROUTED TO MANAGER) ---

@app1.on_message(filters.command("forward_movie") & filters.create(only_admin))
async def cmd_fwd_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/forward_movie <target> [limit]`")
    limit = m.command[2] if len(m.command) > 2 else None
    await multi_session_forwarding_manager(m, DB_FILES["movie_source"], DB_FILES["movie_target"], m.command[1], limit)

@app1.on_message(filters.command("forward_full") & filters.create(only_admin))
async def cmd_fwd_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/forward_full <target> [limit]`")
    limit = m.command[2] if len(m.command) > 2 else None
    await multi_session_forwarding_manager(m, DB_FILES["full_source"], DB_FILES["full_target"], m.command[1], limit)

# --- RUNNER ---
if __name__ == "__main__":
    start_web_server()
    print("🤖 Initializing Clients...")
    
    # Start Clients Composition
    app1.start()
    if app2: app2.start()
    if app3: app3.start()
    
    print(f"✅ Bot Started. Active Workers: {len(WORKER_CLIENTS)}")
    
    # Idle Loop to keep main process alive
    from pyrogram import idle
    idle()
    
    # Cleanup
    app1.stop()
    if app2: app2.stop()
    if app3: app3.stop()
