from keep_alive import keep_alive
keep_alive()

import os, re, json, asyncio
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, 
    PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, 
    RPCError, UsernameInvalid, ChannelPrivate
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- CONFIGURATION ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION1 = os.getenv("SESSION_STRING")       # Boss Account (Commands yahi chalenge)
SESSION2 = os.getenv("SESSION_STRING_2")     # Worker Account (Sirf help karega)
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# --- INITIALIZE DUAL CLIENTS ---
# Boss: Commands sunega aur kaam karega
bot1 = Client("boss_session", api_id=API_ID, api_hash=API_HASH, session_string=SESSION1)
# Worker: Sirf forwarding me help karega
bot2 = Client("worker_session", api_id=API_ID, api_hash=API_HASH, session_string=SESSION2)

# --- GLOBAL STATE ---
GLOBAL_TASK_RUNNING = False
PER_MSG_DELAY = 1.0       # Safe delay per bot
BATCH_SIZE = 100          # Break lene se pehle kitne msg karein
BREAK_TIME = 30           # Break duration in seconds

# --- FILES ---
DB_FILES = {
    "movie_index": "db_movie_index.json",
    "movie_target": "db_movie_target.json",
    "series_index": "db_series_index.json",
    "series_target": "db_series_target.json",
    "full_index": "db_full_index.json",
    "full_target": "db_full_target.json",
    "bad_quality": "db_bad_quality.json",
    "history": "db_forwarded_history.txt"
}

# --- IN-MEMORY DUPLICATE CACHE ---
# Sets use hashing, so lookup is instant (O(1))
processed_unique_ids = set()
processed_name_size = set()

# --- REGEX PATTERNS ---
SERIES_REGEX = re.compile(r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE)
BAD_QUALITY_REGEX = re.compile(
    r"\b(?:cam|camrip|hdcam|ts|telesync|tc|pre-dvdrip|scr|screener|line audio|bad audio)\b", 
    re.IGNORECASE
)
EPISODE_INFO_REGEX = re.compile(
    r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(?:E|Ep|Episode)\s*(\d{1,3})", 
    re.IGNORECASE | re.DOTALL
)

# --- HELPER FUNCTIONS ---

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def load_duplicates():
    """Load history to prevent duplicate forwarding across restarts."""
    global processed_unique_ids, processed_name_size
    processed_unique_ids.clear()
    processed_name_size.clear()
    
    # Load from text history
    if os.path.exists(DB_FILES["history"]):
        with open(DB_FILES["history"], "r") as f:
            for line in f:
                processed_unique_ids.add(line.strip())

    # Load from all target JSONs to be safe
    for key in ["movie_target", "series_target", "full_target"]:
        if os.path.exists(DB_FILES[key]):
            try:
                with open(DB_FILES[key], "r") as f:
                    data = json.load(f)
                    processed_unique_ids.update(data.get("unique_ids", []))
                    processed_name_size.update(data.get("compound_keys", []))
            except: pass
    print(f"✅ Loaded {len(processed_unique_ids)} Unique IDs & {len(processed_name_size)} Name+Size keys.")

def save_forwarded(unique_id, name, size):
    """Save forwarded item instantly to history."""
    if unique_id:
        processed_unique_ids.add(unique_id)
        with open(DB_FILES["history"], "a") as f:
            f.write(f"{unique_id}\n")
    if name and size:
        processed_name_size.add(f"{name}-{size}")

def get_media_details(m):
    """Extract File Name, Size, and Unique ID safely."""
    media = m.video or m.document or m.audio
    if not media: return None, None, None
    return getattr(media, 'file_name', None), getattr(media, 'file_size', 0), getattr(media, 'file_unique_id', None)

async def resolve_chat(client, chat_ref):
    """Smart Chat Resolver (Username, ID, or Link)."""
    try:
        return await client.get_chat(chat_ref)
    except (PeerIdInvalid, UsernameInvalid):
        # Agar simple ID/Username fail ho, toh join karke try karo
        if "t.me" in str(chat_ref):
            try:
                return await client.join_chat(chat_ref)
            except UserAlreadyParticipant:
                pass
        raise ValueError("Chat access nahi mila. Ensure Bot/User is in the chat.")

# --- INDEXING ENGINES (SMART & ROBUST) ---

async def generic_indexer(client, message, chat_ref, db_file, mode="all"):
    """
    Universal Indexer using get_chat_history for 100% coverage.
    Modes: 'all' (Full), 'movie' (Skip series), 'series' (Only series), 'bad' (Bad quality)
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply(f"⏳ **Smart Indexing Started**\nMode: `{mode.upper()}`\nFetching history via `get_chat_history` (No skip)...")
    
    try:
        chat = await resolve_chat(client, chat_ref)
        data_list = []
        found_count = 0
        scanned_count = 0
        
        # Generator approach for memory efficiency
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING:
                await status.edit("🛑 Task Stopped.")
                return

            scanned_count += 1
            if scanned_count % 1000 == 0:
                try: await status.edit(f"🔍 Scanned: {scanned_count} msgs\nFound: {found_count} valid media")
                except: pass

            if not (m.video or m.document):
                continue

            file_name, file_size, unique_id = get_media_details(m)
            if not unique_id: continue

            caption = m.caption or ""
            text_check = f"{file_name} {caption}"
            is_series = bool(SERIES_REGEX.search(text_check))
            
            # --- FILTER LOGIC ---
            if mode == "movie":
                if is_series: continue # Skip Series
            elif mode == "series":
                if not is_series: continue # Skip Movies
            elif mode == "bad":
                if not BAD_QUALITY_REGEX.search(text_check): continue # Skip Good Quality
            
            # --- ADD TO LIST ---
            # Parsing Web Series info if needed
            meta = {}
            if mode == "series":
                match = EPISODE_INFO_REGEX.search(text_check)
                if match:
                    meta = {"name": match.group(1).strip(), "season": int(match.group(2)), "episode": int(match.group(3))}
                else:
                    meta = {"name": file_name, "season": 1, "episode": 999} # Fallback

            data_list.append({
                "msg_id": m.id,
                "chat_id": chat.id,
                "unique_id": unique_id,
                "name": file_name,
                "size": file_size,
                "meta": meta
            })
            found_count += 1

        # --- SORTING & SAVING ---
        await status.edit("⏳ Scanning Complete. Sorting & Saving...")
        
        # Full/Movie: Oldest to Newest
        if mode in ["all", "movie", "bad"]:
            data_list.reverse()
        # Series: Sort by Name -> Season -> Episode
        elif mode == "series":
            data_list.sort(key=lambda x: (x['meta'].get('name', ''), x['meta'].get('season', 0), x['meta'].get('episode', 0)))

        with open(db_file, "w", encoding="utf-8") as f:
            json.dump(data_list, f, indent=2)

        await status.edit(f"✅ **Index Saved!**\nFile: `{db_file}`\nTotal Items: {found_count}")

    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

async def target_indexer(client, message, chat_ref, db_file):
    """Index target channel to update duplicate database."""
    status = await message.reply("⏳ **Target Indexing...**")
    try:
        chat = await resolve_chat(client, chat_ref)
        u_ids = set()
        c_keys = set()
        
        async for m in client.get_chat_history(chat.id):
            file_name, file_size, unique_id = get_media_details(m)
            if unique_id: u_ids.add(unique_id)
            if file_name and file_size: c_keys.add(f"{file_name}-{file_size}")
        
        with open(db_file, "w") as f:
            json.dump({"unique_ids": list(u_ids), "compound_keys": list(c_keys)}, f)
        
        load_duplicates() # Reload RAM
        await status.edit(f"✅ **Target Synced!**\nIDs: {len(u_ids)}\nKeys: {len(c_keys)}")
    except Exception as e:
        await status.edit(f"❌ Error: {e}")

# --- DUAL-CORE FORWARDING ENGINE ---

async def forward_worker(client, target_id, batch, worker_name, status_msg, progress_dict):
    """Single Worker Process."""
    local_count = 0
    
    for item in batch:
        if not GLOBAL_TASK_RUNNING: break
        
        msg_id = item['msg_id']
        src_id = item['chat_id']
        unique_id = item.get('unique_id')
        name = item.get('name')
        size = item.get('size')
        
        # Double Check Duplicates (Real-time)
        compound_key = f"{name}-{size}"
        if unique_id in processed_unique_ids or compound_key in processed_name_size:
            continue

        try:
            # Forward Logic
            await client.forward_messages(target_id, src_id, msg_id)
            save_forwarded(unique_id, name, size)
            
            local_count += 1
            progress_dict[worker_name] += 1
            
            # Batch Break Logic
            if local_count % BATCH_SIZE == 0:
                await asyncio.sleep(BREAK_TIME)
            else:
                await asyncio.sleep(PER_MSG_DELAY)

        except FloodWait as e:
            print(f"[{worker_name}] FloodWait: {e.value}s")
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            print(f"[{worker_name}] Error Msg {msg_id}: {e}")

async def dual_forwarder(message, db_file, target_ref, limit=None):
    """
    The Boss Function.
    Splits data between Session 1 & 2 and manages them.
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("🚀 **Preparing Dual-Core Forwarding...**")
    
    # 1. Load Data
    if not os.path.exists(db_file):
        await status.edit("❌ DB file missing. Run index command first.")
        GLOBAL_TASK_RUNNING = False
        return

    with open(db_file, "r") as f:
        data = json.load(f)

    # 2. Filter Duplicates BEFORE Split
    load_duplicates()
    clean_data = []
    for item in data:
        u_id = item.get('unique_id')
        key = f"{item.get('name')}-{item.get('size')}"
        if u_id not in processed_unique_ids and key not in processed_name_size:
            clean_data.append(item)
    
    if limit:
        clean_data = clean_data[:int(limit)]
        
    total_items = len(clean_data)
    if total_items == 0:
        await status.edit("✅ Nothing new to forward (All duplicates).")
        GLOBAL_TASK_RUNNING = False
        return

    # 3. Resolve Target
    try:
        tgt = await resolve_chat(bot1, target_ref)
        tgt_id = tgt.id
        # Ensure Bot2 is also in chat (Try to get chat info)
        try: await bot2.get_chat(tgt_id)
        except: await status.edit(f"⚠️ **Warning:** Worker Account (Session 2) is not in target chat `{tgt.title}`.")
    except Exception as e:
        await status.edit(f"❌ Target Error: {e}")
        GLOBAL_TASK_RUNNING = False
        return

    # 4. Smart Split (Odd/Even strategy for maximum speed)
    # List 1 gets index 0, 2, 4... | List 2 gets index 1, 3, 5...
    list_1 = clean_data[0::2]
    list_2 = clean_data[1::2]

    await status.edit(
        f"⚡ **Dual-Core Engine Started**\n"
        f"Total Files: `{total_items}`\n"
        f"🤖 Boss Task: `{len(list_1)}` files\n"
        f"🤖 Worker Task: `{len(list_2)}` files\n"
        f"Target: `{tgt.title}`"
    )

    # 5. Run Parallel Tasks
    progress = {"Boss": 0, "Worker": 0}
    
    # Status Updater Task
    async def update_ui():
        while GLOBAL_TASK_RUNNING:
            done = progress["Boss"] + progress["Worker"]
            if done >= total_items: break
            try:
                await status.edit(
                    f"⚡ **Forwarding via Dual Sessions**\n"
                    f"Total Progress: `{done} / {total_items}`\n\n"
                    f"1️⃣ Boss: `{progress['Boss']}`\n"
                    f"2️⃣ Worker: `{progress['Worker']}`",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 STOP", callback_data="stop")]])
                )
            except: pass
            await asyncio.sleep(4)

    ui_task = asyncio.create_task(update_ui())
    
    # Forwarding Tasks
    task1 = asyncio.create_task(forward_worker(bot1, tgt_id, list_1, "Boss", status, progress))
    task2 = asyncio.create_task(forward_worker(bot2, tgt_id, list_2, "Worker", status, progress))
    
    await asyncio.gather(task1, task2)
    GLOBAL_TASK_RUNNING = False
    await ui_task
    
    await status.edit(
        f"✅ **Dual-Core Mission Complete!**\n"
        f"Total Forwarded: `{progress['Boss'] + progress['Worker']}` files."
    )

# --- COMMANDS ---

@bot1.on_message(filters.command("start") & filters.create(only_admin))
async def start_msg(_, m):
    txt = (
        "🔥 **ULTIMATE DUAL-CORE FORWARDER** 🔥\n\n"
        "**Movies:**\n"
        "`/index <channel>` - Index Movies (Skips Series)\n"
        "`/index_target <channel>` - Index Target (Update Dupes)\n\n"
        "**Full Backup:**\n"
        "`/index_full <channel>` - Index Everything (Old to New)\n"
        "`/index_target_full <channel>` - Index Target Full\n\n"
        "**Web Series:**\n"
        "`/index_series <channel>` - Index & Sort Series\n"
        "`/index_target_series <channel>` - Index Target Series\n\n"
        "**Advanced:**\n"
        "`/find_bad <channel>` - Find CamRip/Bad Audio\n"
        "`/forward <db_type> <target> [limit]`\n"
        "   *Types:* `movie`, `full`, `series`, `bad`\n\n"
        "**Utilities:**\n"
        "`/stop` - Emergency Stop\n"
        "`/ping` - Check both clients"
    )
    await m.reply(txt)

@bot1.on_message(filters.command("stop") & filters.create(only_admin))
async def stop_handler(_, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 **Stopping All Engines...** (Finishing current processes)")

@bot1.on_callback_query(filters.regex("stop"))
async def stop_cb(_, q):
    global GLOBAL_TASK_RUNNING
    if q.from_user.id != ADMIN_ID: return
    GLOBAL_TASK_RUNNING = False
    await q.answer("Stopping...")
    await q.message.edit("🛑 **STOPPED BY USER**")

@bot1.on_message(filters.command("ping") & filters.create(only_admin))
async def ping_handler(_, m):
    try:
        me1 = await bot1.get_me()
        me2 = await bot2.get_me()
        await m.reply(f"✅ **System Stable**\nBot 1: `{me1.first_name}`\nBot 2: `{me2.first_name}`")
    except Exception as e:
        await m.reply(f"⚠️ **Error:** One session is dead.\nTrace: {e}")

# --- INDEX COMMANDS MAPPING ---

@bot1.on_message(filters.command("index") & filters.create(only_admin))
async def cmd_idx_movie(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index @channel`")
    await generic_indexer(c, m, m.command[1], DB_FILES["movie_index"], mode="movie")

@bot1.on_message(filters.command("index_target") & filters.create(only_admin))
async def cmd_idx_tgt_movie(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target @channel`")
    await target_indexer(c, m, m.command[1], DB_FILES["movie_target"])

@bot1.on_message(filters.command("index_full") & filters.create(only_admin))
async def cmd_idx_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_full @channel`")
    await generic_indexer(c, m, m.command[1], DB_FILES["full_index"], mode="all")

@bot1.on_message(filters.command("index_target_full") & filters.create(only_admin))
async def cmd_idx_tgt_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target_full @channel`")
    await target_indexer(c, m, m.command[1], DB_FILES["full_target"])

@bot1.on_message(filters.command("index_series") & filters.create(only_admin))
async def cmd_idx_series(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_series @channel`")
    await generic_indexer(c, m, m.command[1], DB_FILES["series_index"], mode="series")

@bot1.on_message(filters.command("index_target_series") & filters.create(only_admin))
async def cmd_idx_tgt_series(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target_series @channel`")
    await target_indexer(c, m, m.command[1], DB_FILES["series_target"])

@bot1.on_message(filters.command("find_bad") & filters.create(only_admin))
async def cmd_find_bad(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/find_bad @channel`")
    await generic_indexer(c, m, m.command[1], DB_FILES["bad_quality"], mode="bad")

# --- MASTER FORWARD COMMAND ---

@bot1.on_message(filters.command("forward") & filters.create(only_admin))
async def cmd_forward(_, m):
    # Syntax: /forward <type> <target> [limit]
    # Types: movie, full, series, bad
    
    if len(m.command) < 3:
        return await m.reply("❌ Usage: `/forward <type> <target_id> [limit]`\nTypes: `movie`, `full`, `series`, `bad`")
    
    db_type = m.command[1].lower()
    target_ref = m.command[2]
    limit = m.command[3] if len(m.command) > 3 else None
    
    db_map = {
        "movie": DB_FILES["movie_index"],
        "full": DB_FILES["full_index"],
        "series": DB_FILES["series_index"],
        "bad": DB_FILES["bad_quality"]
    }
    
    if db_type not in db_map:
        return await m.reply("❌ Invalid Type. Use: `movie`, `full`, `series`, or `bad`")
    
    # Trigger Dual Core Logic
    await dual_forwarder(m, db_map[db_type], target_ref, limit)


# --- MAIN EXECUTION ---
async def main():
    print("🤖 Starting Boss Session...")
    await bot1.start()
    print("👷 Starting Worker Session...")
    await bot2.start()
    
    # Load initial duplicates
    load_duplicates()
    
    print("🔥 Dual-Core Bot Ready! Send /start to Boss.")
    await idle()
    
    await bot1.stop()
    await bot2.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
