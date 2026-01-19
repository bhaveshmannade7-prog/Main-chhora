import os, re, json, asyncio, time, math
from threading import Thread
from flask import Flask
from pyrogram import Client, filters, enums, compose, idle
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

# EXTENSION: Multi-Session Support (3 Sessions)
# Smart Fallback: Checks SESSION1, then SESSION_STRING for backward compatibility
SESSION1 = os.getenv("SESSION1", os.getenv("SESSION_STRING"))
SESSION2 = os.getenv("SESSION2")
SESSION3 = os.getenv("SESSION3")

if not SESSION1:
    print("CRITICAL ERROR: SESSION1 or SESSION_STRING is missing!")
    exit(1)

# --- WEB SERVER FOR RENDER (DEPLOYMENT MODES) ---
app_web = Flask(__name__)

@app_web.route('/')
def home():
    return "✅ Ultra Bot V3 is Running! System Status: Nominal."

def run_web_server():
    port = int(os.getenv("PORT", 8080))
    app_web.run(host="0.0.0.0", port=port)

def start_web_server():
    # AUTO-DETECT: Server vs Mobile
    # If PORT or RENDER env exists, assume Cloud Server.
    if os.getenv("PORT") or os.getenv("RENDER"):
        print("🌍 Cloud Deployment Detected: Starting Web Server...")
        t = Thread(target=run_web_server)
        t.start()
    else:
        print("📱 Mobile/Termux Mode Detected: Web Server Disabled (Saving RAM).")

# --- ADVANCED CLIENT SETUP (MULTI-SESSION) ---
# Primary Client (Session 1) - Commander
app = Client(
    "advanced_user_bot_1", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    session_string=SESSION1, 
    in_memory=True,
    ipv6=False
)

# Secondary Client (Session 2) - Worker
app2 = Client(
    "advanced_user_bot_2", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    session_string=SESSION2 if SESSION2 else SESSION1, 
    in_memory=True,
    ipv6=False
)

# Tertiary Client (Session 3) - Worker
app3 = Client(
    "advanced_user_bot_3", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    session_string=SESSION3 if SESSION3 else SESSION1, 
    in_memory=True,
    ipv6=False
)

# Active Client Manager
ALL_CLIENTS = [app]
if SESSION2: ALL_CLIENTS.append(app2)
if SESSION3: ALL_CLIENTS.append(app3)

# --- GLOBAL SETTINGS (STRICTLY UPGRADED) ---
GLOBAL_TASK_RUNNING = False
PER_MSG_DELAY = 0.2        # Fast speed for normal ops
BATCH_SIZE = 250           # Strict: 250 Messages
BREAK_TIME = 35            # Strict: 35 Seconds Break (Safety)

# --- DATABASE FILES ---
DB_FILES = {
    "movie_source": "db_movie_source.json",
    "movie_target": "db_movie_target.json",
    "full_source": "db_full_source.json",
    "full_target": "db_full_target.json",
    "history": "history_ids.txt"
}

# --- MEMORY CACHE (High Speed Lookup) ---
target_cache = {
    "unique_ids": set(),
    "name_size": set()
}

# --- REGEX PATTERNS ---
BAD_QUALITY_REGEX = re.compile(r"\b(cam|camrip|hdcam|ts|telesync|tc|pre-dvdrip|scr|screener|bad audio)\b", re.IGNORECASE)
SERIES_REGEX = re.compile(r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE)

# --- HELPER FUNCTIONS ---

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def get_file_size_str(filepath):
    """Returns file size in KB or MB."""
    if not os.path.exists(filepath): return "0 KB"
    size = os.path.getsize(filepath)
    if size < 1024: return f"{size} B"
    elif size < 1024 * 1024: return f"{round(size/1024, 2)} KB"
    else: return f"{round(size/(1024*1024), 2)} MB"

def get_media_details(m):
    """
    Highly Optimized Media Extractor.
    Accepts Video and Document (Video files).
    """
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
    """
    Advanced Chat Resolver.
    Handles: IDs, Usernames, Invite Links.
    """
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
        raise ValueError(f"❌ Chat resolve nahi hua. ID/Username check karein.\nError: {e}")

def load_target_cache(db_file):
    """
    Double Layer Caching for 100% Duplicate Protection.
    """
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

    print(f"✅ Cache Loaded: {len(target_cache['unique_ids'])} Unique IDs | {len(target_cache['name_size'])} Keys")

def save_history(unique_id, name, size):
    """Updates memory and file instantly."""
    if unique_id:
        target_cache["unique_ids"].add(unique_id)
        with open(DB_FILES["history"], "a") as f:
            f.write(f"{unique_id}\n")
            
    if name and size:
        target_cache["name_size"].add(f"{name}-{size}")

# --- ENGINES ---

async def indexing_engine(client, message, chat_ref, db_file, mode="all"):
    """
    🚀 High-Speed Indexing Engine (Generates Single Source of Truth)
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply(f"🚀 **High-Speed Indexing Started**\n\nTarget: `{chat_ref}`\nMode: `{mode.upper()}`\n\n_Scanning..._")
    
    try:
        chat = await resolve_chat_id(client, chat_ref)
        data_list = []
        unique_ids_set = set()
        name_size_set = set()
        
        count = 0
        found_movies = 0
        
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING:
                await status.edit("🛑 Task Stopped by User.")
                return

            if not (m.video or m.document):
                continue
            
            file_name, file_size, unique_id = get_media_details(m)
            if not unique_id: continue

            if "target" in db_file:
                unique_ids_set.add(unique_id)
                if file_name and file_size:
                    name_size_set.add(f"{file_name}-{file_size}")
            else:
                data_list.append({
                    "msg_id": m.id,
                    "chat_id": chat.id,
                    "unique_id": unique_id,
                    "name": file_name,
                    "size": file_size
                })
            
            found_movies += 1
            count += 1
            
            if count % 500 == 0:
                try:
                    await status.edit(f"⚡ **Scanning `{chat.title}`**\n\nChecked: {count}\nMedia Found: {found_movies}")
                except FloodWait: pass

        if "target" in db_file:
            final_data = {
                "unique_ids": list(unique_ids_set),
                "compound_keys": list(name_size_set)
            }
            with open(db_file, "w") as f:
                json.dump(final_data, f)
            msg = f"✅ **Target Indexing Complete!**\n\nChannel: `{chat.title}`\nUnique Files: `{len(unique_ids_set)}`\n\nSaved to `{db_file}`."
        else:
            data_list.reverse()
            with open(db_file, "w") as f:
                json.dump(data_list, f, indent=2)
            msg = f"✅ **Source Indexing Complete!**\n\nChannel: `{chat.title}`\nMedia Found: `{len(data_list)}`\n\nSaved to `{db_file}`."

        await status.edit(msg)

    except Exception as e:
        await status.edit(f"❌ Error during Indexing: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

async def forwarding_engine(message, source_db, target_db, destination_ref, limit=None, mode_copy=True):
    """
    🚀 ULTRA SMART FORWARDING ENGINE (ISOLATED FLOODWAIT)
    - Partitions data exactly between available sessions.
    - If Session A hits FloodWait, Session B and C KEEP RUNNING.
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("⚙️ **Initializing Multi-Session Engine...**\nVerifying Safety Protocols...")
    
    # 1. Validation
    if not os.path.exists(source_db):
        return await status.edit("❌ Source Index nahi mila! Pehle `/index` command chalao.")
    
    # 2. Load Source
    try:
        with open(source_db, "r") as f:
            source_data = json.load(f)
    except Exception as e:
        return await status.edit(f"❌ Database Error: {e}")

    # 3. Load Target
    load_target_cache(target_db)
    
    # 4. Resolve Destination (Main Client)
    try:
        dest_chat = await resolve_chat_id(app, destination_ref)
        dest_id = dest_chat.id
    except Exception as e:
        return await status.edit(f"❌ Destination Error: {e}")

    # 5. Filter Duplicates
    await status.edit("🔍 **Filtering Duplicates...**")
    final_list = []
    skipped_count = 0
    
    for item in source_data:
        u_id = item.get("unique_id")
        name = item.get("name")
        size = item.get("size")
        key = f"{name}-{size}"
        
        if (u_id in target_cache["unique_ids"]) or (key in target_cache["name_size"]):
            skipped_count += 1
            continue
        final_list.append(item)
    
    if limit and int(limit) > 0:
        final_list = final_list[:int(limit)]
    
    if not final_list:
        return await status.edit(f"✅ **Job Done!**\nNo new files to forward.\nDuplicates Skipped: `{skipped_count}`")

    # 6. PARTITIONING (SPLITTING DATA)
    total_items = len(final_list)
    active_sessions = [c for c in ALL_CLIENTS if c.is_connected]
    num_sessions = len(active_sessions)
    
    # Calculate Chunk Size
    chunk_size = math.ceil(total_items / num_sessions)
    chunks = [final_list[i:i + chunk_size] for i in range(0, total_items, chunk_size)]
    
    # Pad chunks if necessary
    while len(chunks) < len(active_sessions):
        chunks.append([])

    await status.edit(
        f"🚀 **Multi-Session Forwarding LAUNCHED!**\n\n"
        f"📦 Total Files: `{total_items}`\n"
        f"🤖 Active Bots: `{num_sessions}`\n"
        f"🗑️ Duplicates Removed: `{skipped_count}`\n"
        f"🛡️ Safety Mode: `{BATCH_SIZE} msgs` -> `{BREAK_TIME}s break`\n\n"
        f"⚡ _All sessions running in parallel. If one waits, others continue._"
    )

    # SHARED STATS (Atomic Update)
    progress_stats = {"success": 0}
    
    # --- ISOLATED WORKER FUNCTION ---
    async def session_worker(client, worker_data, session_name):
        local_batch = 0
        
        for item in worker_data:
            if not GLOBAL_TASK_RUNNING:
                break
            
            try:
                if mode_copy:
                    await client.copy_message(dest_id, item['chat_id'], item['msg_id'])
                else:
                    await client.forward_messages(dest_id, item['chat_id'], item['msg_id'])
                
                # Save History (Instant)
                save_history(item.get("unique_id"), item.get("name"), item.get("size"))
                
                progress_stats["success"] += 1
                local_batch += 1
                
                # Update UI (Only Main Client does this to avoid conflict)
                if progress_stats["success"] % 25 == 0:
                    try:
                        await status.edit(f"🔄 **Forwarding in Progress...**\n\nSent: `{progress_stats['success']}` / `{total_items}`")
                    except Exception: pass
                
                # --- SAFETY BREAK LOGIC (ISOLATED) ---
                if local_batch >= BATCH_SIZE:
                    print(f"[{session_name}] 🛡️ Safety Break: {BREAK_TIME}s")
                    await asyncio.sleep(BREAK_TIME)
                    local_batch = 0 # Reset batch
                else:
                    await asyncio.sleep(PER_MSG_DELAY)

            except FloodWait as e:
                print(f"[{session_name}] ⏳ FLOODWAIT: Sleeping {e.value}s")
                # CRITICAL: Only THIS session sleeps. Others continue.
                await asyncio.sleep(e.value + 5) 
            except (MessageIdInvalid, MessageAuthorRequired):
                print(f"[{session_name}] Skip Invalid Msg")
            except Exception as e:
                print(f"[{session_name}] Error: {e}")

    # 7. EXECUTE PARALLEL TASKS
    tasks = []
    for i, session in enumerate(active_sessions):
        if i < len(chunks):
            task_name = f"Session-{i+1}"
            tasks.append(session_worker(session, chunks[i], task_name))
    
    await asyncio.gather(*tasks)

    GLOBAL_TASK_RUNNING = False
    await status.edit(
        f"🎉 **Mission Accomplished!**\n\n"
        f"✅ Total Forwarded: `{progress_stats['success']}`\n"
        f"🗑️ Duplicates Skipped: `{skipped_count}`\n"
        f"📁 Target: `{dest_chat.title}`"
    )

# --- COMMANDS ---

@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_msg(_, m):
    txt = (
        "🤖 **Ultra Advanced Bot V3 (Manager)**\n"
        "_(Multi-Session | Partitioning | Smart Safety)_\n\n"
        "**📚 Indexing**\n"
        "`/index <channel>` - Source Movie Indexing.\n"
        "`/index_full <channel>` - Source Full Indexing.\n"
        "`/index_target <channel>` - Target Duplicate Check.\n\n"
        "**🚀 Forwarding (3 Sessions Parallel)**\n"
        "`/forward_movie <target> [limit]`\n"
        "`/forward_full <target> [limit]`\n\n"
        "**🛠️ Maintenance**\n"
        "`/stats` - Smart Dashboard (Size & Count).\n"
        "`/del_db <db_name>` - Delete specific JSON.\n"
        "`/stop` - Emergency Stop.\n"
        "`/sync` - Fix Chat IDs."
    )
    await m.reply(txt)

@app.on_message(filters.command("stats") & filters.create(only_admin))
async def stats_cmd(_, m):
    """
    SMART STATS: Shows existance, entry count, and file size on disk.
    """
    report = "📊 **Smart Database Statistics**\n\n"
    
    # 1. Database Files Check
    report += "**📂 Database Files:**\n"
    for key, path in DB_FILES.items():
        if os.path.exists(path):
            size_str = get_file_size_str(path)
            try:
                # Count lines or entries
                if path.endswith(".json"):
                    with open(path, 'r') as f:
                        data = json.load(f)
                        if isinstance(data, list): count = len(data)
                        elif isinstance(data, dict): count = len(data.get("unique_ids", []))
                        else: count = "Unknown"
                else:
                    # History file is line based
                    with open(path, 'r') as f:
                        count = sum(1 for _ in f)
            except: count = "Error"
            
            report += f"✅ `{key}`: **{count} items** ({size_str})\n"
        else:
            report += f"❌ `{key}`: _Not Found_\n"

    # 2. Memory Cache
    report += "\n**🧠 RAM Cache:**\n"
    report += f"Unique IDs: `{len(target_cache['unique_ids'])}`\n"
    report += f"Name Keys: `{len(target_cache['name_size'])}`\n"

    # 3. Session Status
    active = [c.name for c in ALL_CLIENTS if c.is_connected]
    report += f"\n**🤖 Bot Status:**\n"
    report += f"Running: `{GLOBAL_TASK_RUNNING}`\n"
    report += f"Sessions: `{len(active)} Active`\n"
    report += f"Safety: `{BATCH_SIZE} msgs` -> `{BREAK_TIME}s break`"

    await m.reply(report)

@app.on_message(filters.command("del_db") & filters.create(only_admin))
async def delete_db_cmd(_, m):
    """
    Deletes a specific JSON database file.
    Usage: /del_db movie_source
    """
    if len(m.command) < 2:
        available = ", ".join([f"`{k}`" for k in DB_FILES.keys()])
        return await m.reply(f"⚠️ **Usage:** `/del_db <db_name>`\n\n**Available Databases:**\n{available}")
    
    key = m.command[1].lower()
    
    if key not in DB_FILES:
        return await m.reply("❌ Invalid Database Name! Check `/stats` for names.")
    
    path = DB_FILES[key]
    
    if os.path.exists(path):
        try:
            os.remove(path)
            # Clear cache if history or target deleted
            if "target" in key or "history" in key:
                target_cache["unique_ids"].clear()
                target_cache["name_size"].clear()
                
            await m.reply(f"🗑️ **Deleted Successfully:** `{path}`\n\nCache has been cleared if necessary.")
        except Exception as e:
            await m.reply(f"❌ Delete Failed: {e}")
    else:
        await m.reply(f"⚠️ File `{path}` does not exist.")

@app.on_message(filters.command("sync") & filters.create(only_admin))
async def sync_cmd(_, m):
    msg = await m.reply("♻️ **Syncing Dialogs (Refreshing Cache)...**")
    try:
        count = 0
        for client in ALL_CLIENTS:
            if client.is_connected:
                async for dialog in client.get_dialogs():
                    count += 1
        await msg.edit(f"✅ **Sync Complete!**\nFound `{count}` chats across sessions.")
    except Exception as e:
        await msg.edit(f"❌ Sync Error: {e}")

@app.on_message(filters.command("stop") & filters.create(only_admin))
async def stop_cmd(_, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 **Emergency Stop!**\nAll sessions halting after current operation.")

# --- INDEX HANDLERS ---

@app.on_message(filters.command("index") & filters.create(only_admin))
async def cmd_idx_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["movie_source"], mode="movie")

@app.on_message(filters.command("index_target") & filters.create(only_admin))
async def cmd_idx_tgt_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["movie_target"], mode="target")

@app.on_message(filters.command("index_full") & filters.create(only_admin))
async def cmd_idx_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_full @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["full_source"], mode="all")

@app.on_message(filters.command("index_target_full") & filters.create(only_admin))
async def cmd_idx_tgt_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target_full @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["full_target"], mode="target")

# --- FORWARD HANDLERS ---

@app.on_message(filters.command("forward_movie") & filters.create(only_admin))
async def cmd_fwd_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/forward_movie <target> [limit]`")
    limit = m.command[2] if len(m.command) > 2 else None
    await forwarding_engine(m, DB_FILES["movie_source"], DB_FILES["movie_target"], m.command[1], limit)

@app.on_message(filters.command("forward_full") & filters.create(only_admin))
async def cmd_fwd_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/forward_full <target> [limit]`")
    limit = m.command[2] if len(m.command) > 2 else None
    await forwarding_engine(m, DB_FILES["full_source"], DB_FILES["full_target"], m.command[1], limit)

# --- MAIN RUNNER ---
if __name__ == "__main__":
    print("🤖 Ultra Bot V3 Initializing...")
    
    # 1. Web Server (Conditional)
    start_web_server() 

    # 2. Start Multi-Sessions
    print(f"🚀 Launching {len(ALL_CLIENTS)} Independent Sessions...")
    compose(ALL_CLIENTS)
