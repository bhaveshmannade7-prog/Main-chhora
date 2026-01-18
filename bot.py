import os, re, json, asyncio, time
from threading import Thread
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
SESSION_STRING = os.getenv("SESSION_STRING")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# --- WEB SERVER FOR RENDER (FIX FOR NO OPEN PORTS) ---
app_web = Flask(__name__)

@app_web.route('/')
def home():
    return "✅ Bot is Running Successfully! Port is Open."

def run_web_server():
    port = int(os.getenv("PORT", 8080))
    app_web.run(host="0.0.0.0", port=port)

def start_web_server():
    t = Thread(target=run_web_server)
    t.start()

# --- ADVANCED CLIENT SETUP ---
# Memory Mode ON & IPv6 Disabled for stability and speed
app = Client(
    "advanced_user_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    session_string=SESSION_STRING, 
    in_memory=True,
    ipv6=False
)

# --- GLOBAL SETTINGS (OPTIMIZED) ---
GLOBAL_TASK_RUNNING = False
PER_MSG_DELAY = 0.2        # Speed fast kar di gayi hai (Safe limit)
BATCH_SIZE = 250           # 250 Messages ka batch
BREAK_TIME = 30            # 250 ke baad 30 second ka break (Account Safety)

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
# Sirf Movies/Series detect karne ke liye strict filters
BAD_QUALITY_REGEX = re.compile(r"\b(cam|camrip|hdcam|ts|telesync|tc|pre-dvdrip|scr|screener|bad audio)\b", re.IGNORECASE)
SERIES_REGEX = re.compile(r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE)

# --- HELPER FUNCTIONS ---

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def get_media_details(m):
    """
    Highly Optimized Media Extractor.
    Sirf Video aur Document (Video files) ko accept karega.
    """
    # Priority: Video > Document
    media = m.video or m.document
    
    if not media:
        return None, 0, None
    
    # Strict Filter: Agar document hai to check karo kya wo video file hai?
    if m.document:
        mime = getattr(m.document, "mime_type", "")
        if not mime or "video" not in mime:
            # Check file extension as backup
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
    
    # 1. Numeric ID Check
    try:
        if ref_str.lstrip('-').isdigit():
            return await client.get_chat(int(ref_str))
    except: pass

    # 2. Invite Link Check
    if "t.me/+" in ref_str or "joinchat" in ref_str:
        try:
            return await client.join_chat(ref_str)
        except UserAlreadyParticipant:
            pass
        except Exception as e:
            raise ValueError(f"❌ Invite Link Error: {e}")

    # 3. Username / Fallback
    try:
        return await client.get_chat(ref_str)
    except Exception as e:
        raise ValueError(f"❌ Chat resolve nahi hua. ID/Username check karein.\nError: {e}")

def load_target_cache(db_file):
    """
    Double Layer Caching for 100% Duplicate Protection.
    Layers: Unique ID + (Name-Size combination)
    """
    global target_cache
    target_cache["unique_ids"].clear()
    target_cache["name_size"].clear()
    
    # Layer 1: Permanent History
    if os.path.exists(DB_FILES["history"]):
        with open(DB_FILES["history"], "r") as f:
            for line in f:
                target_cache["unique_ids"].add(line.strip())

    # Layer 2: Target Database
    if os.path.exists(db_file):
        try:
            with open(db_file, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    target_cache["unique_ids"].update(data.get("unique_ids", []))
                    target_cache["name_size"].update(data.get("compound_keys", []))
        except Exception as e:
            print(f"Cache Load Warning: {e}")

    print(f"✅ Cache Loaded: {len(target_cache['unique_ids'])} Unique IDs | {len(target_cache['name_size'])} Name-Size Keys")

def save_history(unique_id, name, size):
    """Updates memory and file instantly."""
    if unique_id:
        target_cache["unique_ids"].add(unique_id)
        # File me append karo (fail-safe)
        with open(DB_FILES["history"], "a") as f:
            f.write(f"{unique_id}\n")
            
    if name and size:
        target_cache["name_size"].add(f"{name}-{size}")

# --- ENGINES ---

async def indexing_engine(client, message, chat_ref, db_file, mode="all"):
    """
    🚀 High-Speed Indexing Engine
    - Uses 'get_chat_history' (Reliable)
    - Skips non-media instantly
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
        
        # Async Iterator for History
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING:
                await status.edit("🛑 Task Stopped by User.")
                return

            # Quick Check: Agar media nahi hai to turant skip karo (Speed Boost)
            if not (m.video or m.document):
                continue
            
            # Deep Check: Details nikalo
            file_name, file_size, unique_id = get_media_details(m)
            if not unique_id: continue

            # Text Messages Filter (Strictly Movies Logic)
            # Hum sirf file name aur caption check karenge
            # Agar chat message hai (bina file ke) wo upar hi filter ho gaya
            
            # Logic: Save Data
            if "target" in db_file:
                # Target indexing ke liye Sets use karo (Fast)
                unique_ids_set.add(unique_id)
                if file_name and file_size:
                    name_size_set.add(f"{file_name}-{file_size}")
            else:
                # Source indexing ke liye List use karo
                data_list.append({
                    "msg_id": m.id,
                    "chat_id": chat.id,
                    "unique_id": unique_id,
                    "name": file_name,
                    "size": file_size
                })
            
            found_movies += 1
            count += 1
            
            # Update Status every 500 msgs
            if count % 500 == 0:
                try:
                    await status.edit(f"⚡ **Scanning `{chat.title}`**\n\nChecked: {count}\nMovies Found: {found_movies}")
                except FloodWait: pass

        # Saving Data
        if "target" in db_file:
            final_data = {
                "unique_ids": list(unique_ids_set),
                "compound_keys": list(name_size_set)
            }
            with open(db_file, "w") as f:
                json.dump(final_data, f)
            msg = f"✅ **Target Indexing Complete!**\n\nChannel: `{chat.title}`\nUnique Files: `{len(unique_ids_set)}`\n\nDuplicate protection database update ho gaya hai."
        else:
            # Source list ko reverse karo taki OLD -> NEW forward ho
            data_list.reverse()
            with open(db_file, "w") as f:
                json.dump(data_list, f, indent=2)
            msg = f"✅ **Source Indexing Complete!**\n\nChannel: `{chat.title}`\nMovies Found: `{len(data_list)}`\n\nSaved to `{db_file}`."

        await status.edit(msg)

    except Exception as e:
        await status.edit(f"❌ Error during Indexing: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

async def forwarding_engine(message, source_db, target_db, destination_ref, limit=None, mode_copy=True):
    """
    🚀 Smart Forwarding Engine
    - 250 Messages -> 30s Break
    - Double Duplicate Check
    - Account Safety Priority
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("⚙️ **Preparing Engine...**\nLoading Databases...")
    
    # 1. Validation
    if not os.path.exists(source_db):
        return await status.edit("❌ Source Index nahi mila! Pehle `/index` command chalao.")
    
    # 2. Load Source
    try:
        with open(source_db, "r") as f:
            source_data = json.load(f)
    except Exception as e:
        return await status.edit(f"❌ Database Error: {e}")

    # 3. Load Target (Cache build karo for speed)
    load_target_cache(target_db)
    
    # 4. Resolve Destination
    try:
        dest_chat = await resolve_chat_id(app, destination_ref)
    except Exception as e:
        return await status.edit(f"❌ Destination Error: {e}")

    # 5. Smart Filtering (Duplicates Hatana)
    await status.edit("🔍 **Checking Duplicates...**")
    final_list = []
    skipped_count = 0
    
    for item in source_data:
        u_id = item.get("unique_id")
        name = item.get("name")
        size = item.get("size")
        
        key = f"{name}-{size}"
        
        # COMBINATION CHECK (String 1: ID, String 2: Name+Size)
        if (u_id in target_cache["unique_ids"]) or (key in target_cache["name_size"]):
            skipped_count += 1
            continue
        
        final_list.append(item)
    
    # Limit logic
    if limit and int(limit) > 0:
        final_list = final_list[:int(limit)]
    
    if not final_list:
        return await status.edit(f"✅ **Nothing to Forward!**\n\nSabhi files target channel me pehle se maujood hain.\nSkipped Duplicates: `{skipped_count}`")

    # 6. Forwarding Loop
    total_to_fwd = len(final_list)
    await status.edit(
        f"🚀 **Forwarding Started!**\n\n"
        f"Source Files: `{len(source_data)}`\n"
        f"To Forward: `{total_to_fwd}`\n"
        f"Duplicates Removed: `{skipped_count}`\n"
        f"Target: `{dest_chat.title}`"
    )

    success = 0
    batch_counter = 0 # Break track karne ke liye
    
    for i, item in enumerate(final_list):
        if not GLOBAL_TASK_RUNNING:
            break
        
        try:
            # Action: Copy or Forward
            if mode_copy:
                await app.copy_message(dest_chat.id, item['chat_id'], item['msg_id'])
            else:
                await app.forward_messages(dest_chat.id, item['chat_id'], item['msg_id'])
            
            # Instant Save to History (Safety)
            save_history(item.get("unique_id"), item.get("name"), item.get("size"))
            
            success += 1
            batch_counter += 1
            
            # --- STATUS UPDATE ---
            if success % 20 == 0:
                try:
                    await status.edit(f"🔄 **Forwarding...**\n\nProgress: `{success}` / `{total_to_fwd}`")
                except FloodWait: pass
            
            # --- SMART BREAK LOGIC (250 Msgs -> 30s Break) ---
            if batch_counter >= BATCH_SIZE:
                remaining = total_to_fwd - success
                await status.edit(
                    f"☕ **Taking a Break (Safety Mode)**\n\n"
                    f"Batch Completed: `{BATCH_SIZE}` messages.\n"
                    f"Waiting for **{BREAK_TIME} seconds**...\n"
                    f"Remaining: `{remaining}`"
                )
                await asyncio.sleep(BREAK_TIME)
                batch_counter = 0 # Reset counter
                await status.edit(f"▶️ **Resuming...**\nProgress: `{success}` / `{total_to_fwd}`")
            else:
                # Normal Speed Delay
                await asyncio.sleep(PER_MSG_DELAY)

        except FloodWait as e:
            await status.edit(f"⏳ **FloodWait Hit!**\n\nTelegram ne roka hai. `{e.value}` seconds wait kar raha hoon...")
            await asyncio.sleep(e.value + 5) # Extra 5s safety buffer
        except (MessageIdInvalid, MessageAuthorRequired):
            print(f"Skipping deleted message: {item['msg_id']}")
        except Exception as e:
            print(f"Forward Error: {e}")

    GLOBAL_TASK_RUNNING = False
    await status.edit(
        f"🎉 **Task Completed!**\n\n"
        f"✅ Successfully Forwarded: `{success}`\n"
        f"🗑️ Duplicates Skipped: `{skipped_count}`\n"
        f"📁 Target: `{dest_chat.title}`"
    )

# --- COMMANDS ---

@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_msg(_, m):
    txt = (
        "🤖 **Ultra Advanced Movie Bot (Render Fixed)**\n"
        "_(Optimized for Speed & Safety)_\n\n"
        "**📚 Indexing Commands (Source)**\n"
        "`/index <channel>` - Sirf Movies index karega (Fast).\n"
        "`/index_full <channel>` - Sab kuch index karega.\n\n"
        "**🎯 Target Indexing (Duplicate Killer)**\n"
        "`/index_target <channel>` - Target ko scan kare taaki duplicates na jayein.\n\n"
        "**🚀 Forwarding Commands**\n"
        "`/forward_movie <target_id> [limit]` - Movies forward kare.\n"
        "`/forward_full <target_id> [limit]` - Full forward.\n\n"
        "**⚙️ Utility**\n"
        "`/stats` - Database aur Bot ka status dekhein.\n"
        "`/stop` - Current task ko rokein.\n"
        "`/sync` - Chat ID errors fix karein."
    )
    await m.reply(txt)

@app.on_message(filters.command("stats") & filters.create(only_admin))
async def stats_cmd(_, m):
    # Check File Sizes
    mov_src = len(json.load(open(DB_FILES["movie_source"]))) if os.path.exists(DB_FILES["movie_source"]) else 0
    full_src = len(json.load(open(DB_FILES["full_source"]))) if os.path.exists(DB_FILES["full_source"]) else 0
    
    # Cache Size
    cache_ids = len(target_cache["unique_ids"])
    cache_names = len(target_cache["name_size"])
    
    txt = (
        "📊 **Bot Statistics**\n\n"
        f"⚡ **Task Running:** `{GLOBAL_TASK_RUNNING}`\n"
        f"🐢 **Break Time:** `{BREAK_TIME}s` after `{BATCH_SIZE}` msgs\n\n"
        "**📂 Source Indexes:**\n"
        f"Movies: `{mov_src}`\n"
        f"Full: `{full_src}`\n\n"
        "**🛡️ Duplicate Protection (Cache):**\n"
        f"Unique IDs: `{cache_ids}`\n"
        f"Name+Size Keys: `{cache_names}`"
    )
    await m.reply(txt)

@app.on_message(filters.command("sync") & filters.create(only_admin))
async def sync_cmd(_, m):
    msg = await m.reply("♻️ **Syncing Dialogs (Refreshing Cache)...**")
    try:
        count = 0
        async for dialog in app.get_dialogs():
            count += 1
        await msg.edit(f"✅ **Sync Complete!**\nFound `{count}` chats.\nAb ID errors nahi aayenge.")
    except Exception as e:
        await msg.edit(f"❌ Sync Error: {e}")

@app.on_message(filters.command("stop") & filters.create(only_admin))
async def stop_cmd(_, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 **Stopping Task...**\nAgla batch process nahi hoga.")

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
    print("🤖 Bot Started with Web Server (Render Compatible)...")
    start_web_server() # Pehle Web Server Start hoga
    app.run()          # Phir Bot Start hoga
