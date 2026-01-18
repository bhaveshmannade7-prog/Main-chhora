from keep_alive import keep_alive
keep_alive()

import os, re, json, asyncio, sys, time, datetime
from pyrogram import Client, filters, idle
from pyrogram.errors import (
    FloodWait, PeerIdInvalid, UserAlreadyParticipant, 
    UsernameInvalid, ChannelPrivate, InviteHashInvalid
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- CONFIGURATION CHECK ---
print("⚙️ System Booting...")

START_TIME = time.time()

try:
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH")
    SESSION1 = os.getenv("SESSION_STRING")       # Boss
    SESSION2 = os.getenv("SESSION_STRING_2")     # Worker
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
    
    if not (API_ID and API_HASH and SESSION1 and ADMIN_ID):
        print("❌ CRITICAL: Env Vars Missing (API_ID, HASH, SESSION, or ADMIN_ID)")
        sys.exit(1)

except ValueError:
    print("❌ Configuration Error: IDs must be numbers.")
    sys.exit(1)

# --- INITIALIZE CLIENTS (Memory Mode for Speed) ---
bot1 = Client("boss", api_id=API_ID, api_hash=API_HASH, session_string=SESSION1, in_memory=True)
bot2 = Client("worker", api_id=API_ID, api_hash=API_HASH, session_string=SESSION2, in_memory=True)

# --- SETTINGS ---
GLOBAL_TASK_RUNNING = False
PER_MSG_DELAY = 0.8       # Optimized Speed
BATCH_SIZE = 200          # Increased Batch
BREAK_TIME = 15           

DB_FILES = {
    "movie": "db_movie_index.json",
    "series": "db_series_index.json",
    "full": "db_full_index.json",
    "history": "db_forwarded_history.txt"
}

# --- CACHE ---
processed_unique_ids = set()
processed_name_size = set()

# --- REGEX ---
SERIES_REGEX = re.compile(r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE)
BAD_QUALITY_REGEX = re.compile(r"\b(cam|camrip|hdcam|ts|telesync|tc|scr|screener)\b", re.IGNORECASE)

# --- UTILS ---

def get_readable_time(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))

def load_duplicates():
    """Load history efficiently."""
    global processed_unique_ids, processed_name_size
    processed_unique_ids.clear()
    processed_name_size.clear()
    
    # Load Text History
    if os.path.exists(DB_FILES["history"]):
        with open(DB_FILES["history"], "r") as f:
            processed_unique_ids.update(line.strip() for line in f)

    print(f"✅ Database Loaded: {len(processed_unique_ids)} items.")

def save_forwarded(unique_id, name, size):
    if unique_id:
        processed_unique_ids.add(unique_id)
        with open(DB_FILES["history"], "a") as f:
            f.write(f"{unique_id}\n")
    if name and size:
        processed_name_size.add(f"{name}-{size}")

async def resolve_chat_smart(client, chat_input):
    """
    Handles Usernames (@channel), IDs (-100xxx), and Invite Links.
    """
    try:
        # 1. Check if Input is Integer ID (as string or int)
        if str(chat_input).lstrip("-").isdigit():
            chat_id = int(chat_input)
            return await client.get_chat(chat_id)
        
        # 2. Check if Username
        if str(chat_input).startswith("@"):
            return await client.get_chat(chat_input)
            
        # 3. Check if Link (t.me)
        if "t.me/" in str(chat_input):
            try:
                return await client.join_chat(chat_input)
            except UserAlreadyParticipant:
                # If already joined, we need to extract username/ID to get_chat
                # This is tricky, so we rely on get_chat failing over
                pass
                
        # 4. Fallback: Try get_chat directly
        return await client.get_chat(chat_input)

    except PeerIdInvalid:
        raise ValueError("❌ Chat ID Invalid or Bot hasn't met this chat. Run /sync.")
    except UsernameInvalid:
        raise ValueError("❌ Invalid Username.")
    except Exception as e:
        raise ValueError(f"❌ Chat access failed: {e}")

# --- GLOBAL SYNC LOGIC (THE FIX) ---
async def global_sync_engine(client, status_msg, bot_name):
    """Iterates through ALL dialogs to refresh Pyrogram's Peer Cache."""
    count = 0
    try:
        async for dialog in client.get_dialogs():
            count += 1
            # Just iterating is enough to cache the Access Hash
    except Exception as e:
        print(f"Sync Error {bot_name}: {e}")
    return count

# --- INDEXER ---
async def universal_indexer(client, message, chat_input, db_key, mode="all"):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply(f"⏳ **Indexing Started...**\nTarget: `{chat_input}`\nMode: `{mode.upper()}`")
    
    try:
        chat = await resolve_chat_smart(client, chat_input)
        data = []
        found = 0
        
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING:
                await status.edit("🛑 Stopped.")
                return

            if not (m.video or m.document): continue
            
            # Smart Extraction
            media = m.video or m.document or m.audio
            fname = getattr(media, 'file_name', "") or ""
            fsize = getattr(media, 'file_size', 0)
            uid = getattr(media, 'file_unique_id', None)
            
            if not uid: continue
            
            # Filtering
            caption = m.caption or ""
            full_text = f"{fname} {caption}"
            is_series = bool(SERIES_REGEX.search(full_text))
            
            if mode == "movie" and is_series: continue
            if mode == "series" and not is_series: continue
            
            data.append({
                "msg_id": m.id,
                "chat_id": chat.id,
                "unique_id": uid,
                "name": fname,
                "size": fsize
            })
            found += 1
            if found % 500 == 0: await status.edit(f"⚡ Scanning... Found: {found}")

        # Sorting: Oldest First for proper forwarding
        data.reverse()
        
        with open(DB_FILES[db_key], "w") as f:
            json.dump(data, f)
            
        await status.edit(f"✅ **Indexing Success!**\n📂 File: `{DB_FILES[db_key]}`\n🔢 Total: `{found}`")
        
    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

# --- DUAL CORE FORWARDER ---
async def forward_engine(client, target_id, batch, name, prog_dict):
    for item in batch:
        if not GLOBAL_TASK_RUNNING: break
        
        uid = item.get('unique_id')
        key = f"{item.get('name')}-{item.get('size')}"
        
        # Real-time Duplicate Check
        if uid in processed_unique_ids or key in processed_name_size:
            continue
            
        try:
            await client.forward_messages(target_id, item['chat_id'], item['msg_id'])
            save_forwarded(uid, item.get('name'), item.get('size'))
            prog_dict[name] += 1
            await asyncio.sleep(PER_MSG_DELAY)
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            print(f"[{name}] Err: {e}")

async def start_forwarding(m, db_key, target_input, limit=None):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    status = await m.reply("🚀 **Initializing Dual-Core...**")
    
    if not os.path.exists(DB_FILES.get(db_key, "")):
        return await status.edit("❌ Index file not found.")

    with open(DB_FILES[db_key], "r") as f:
        raw_data = json.load(f)

    # Filter Duplicates BEFORE splitting
    load_duplicates()
    clean_data = [
        d for d in raw_data 
        if d['unique_id'] not in processed_unique_ids 
        and f"{d.get('name')}-{d.get('size')}" not in processed_name_size
    ]
    
    if limit: clean_data = clean_data[:int(limit)]
    if not clean_data: return await status.edit("✅ All files already forwarded!")

    try:
        tgt = await resolve_chat_smart(bot1, target_input)
        # Ensure Bot2 can see target
        try: await bot2.get_chat(tgt.id)
        except: await status.edit(f"⚠️ **Worker Bot** cannot access Target. Add Bot 2 to admin.")
    except Exception as e:
        return await status.edit(f"❌ Target Error: {e}")

    # Odd-Even Split
    task1_data = clean_data[0::2]
    task2_data = clean_data[1::2]
    total = len(clean_data)
    
    prog = {"Boss": 0, "Worker": 0}
    
    # UI Updater
    async def ui_loop():
        while GLOBAL_TASK_RUNNING:
            done = prog["Boss"] + prog["Worker"]
            if done >= total: break
            try:
                await status.edit(
                    f"⚡ **Forwarding in Progress**\n"
                    f"📊 Progress: `{done} / {total}`\n"
                    f"🤖 Boss: `{prog['Boss']}` | 👷 Worker: `{prog['Worker']}`",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 STOP", "stop")]])
                )
            except: pass
            await asyncio.sleep(4)

    asyncio.create_task(ui_loop())
    
    # Run Both Bots
    await asyncio.gather(
        forward_engine(bot1, tgt.id, task1_data, "Boss", prog),
        forward_engine(bot2, tgt.id, task2_data, "Worker", prog)
    )
    
    GLOBAL_TASK_RUNNING = False
    await status.edit(f"✅ **Batch Completed!**\nTotal Sent: {prog['Boss'] + prog['Worker']}")

# --- COMMANDS ---

@bot1.on_message(filters.command("start") & filters.user(ADMIN_ID))
async def start(_, m):
    await m.reply(
        "🔰 **ADVANCED DUAL-BOT CONTROLLER** 🔰\n\n"
        "🔄 **System Sync:**\n"
        "`/sync` - Auto-refresh both bots (Fixes 'Chat not found')\n"
        "`/stats` - View System Health & Uptime\n\n"
        "📂 **Indexing:**\n"
        "`/index <username/id>` - Movies Only\n"
        "`/index_series <username/id>` - Series Only\n"
        "`/index_full <username/id>` - Everything\n\n"
        "🚀 **Forwarding:**\n"
        "`/forward movie <target>`\n"
        "`/forward series <target>`\n"
        "`/forward full <target>`\n"
        "*Usage: Target can be @channel or -100123456*"
    )

@bot1.on_message(filters.command("sync") & filters.user(ADMIN_ID))
async def sync_handler(c, m):
    """Global Sync: Refreshes Dialogs for BOTH bots."""
    msg = await m.reply("♻️ **Global Sync Started...**\nConnecting to all cached chats...")
    
    start = time.time()
    
    # Run get_dialogs for both concurrently
    task1 = asyncio.create_task(global_sync_engine(bot1, msg, "Boss"))
    task2 = asyncio.create_task(global_sync_engine(bot2, msg, "Worker"))
    
    results = await asyncio.gather(task1, task2)
    end = time.time()
    
    txt = (
        f"✅ **Sync Complete!**\n"
        f"⏱ Time: `{round(end-start, 2)}s`\n\n"
        f"🤖 Boss Cached: `{results[0]}` chats\n"
        f"👷 Worker Cached: `{results[1]}` chats\n\n"
        "💡 *Ab aap `/index` command chala sakte hain, ID error nahi aayega.*"
    )
    await msg.edit(txt)

@bot1.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_handler(c, m):
    msg = await m.reply("📊 **Fetching System Stats...**")
    
    # Calculate Ping
    start = time.time()
    await bot1.get_me()
    ping1 = round((time.time() - start) * 1000, 2)
    
    start = time.time()
    await bot2.get_me()
    ping2 = round((time.time() - start) * 1000, 2)
    
    # DB Stats
    history_count = len(processed_unique_ids)
    uptime = get_readable_time(time.time() - START_TIME)
    
    txt = (
        "📊 **SYSTEM STATUS DASHBOARD**\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"⏱ **Uptime:** `{uptime}`\n"
        f"💾 **History DB:** `{history_count}` files\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🤖 **Boss Bot:**\n"
        f"   - Ping: `{ping1}ms`\n"
        f"   - Status: ✅ Online\n"
        f"👷 **Worker Bot:**\n"
        f"   - Ping: `{ping2}ms`\n"
        f"   - Status: ✅ Online\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "✅ *Server is Healthy*"
    )
    await msg.edit(txt)

@bot1.on_message(filters.command("index") & filters.user(ADMIN_ID))
async def cmd_idx_movie(c, m):
    if len(m.command) < 2: return await m.reply("Example: `/index -100123456`")
    await universal_indexer(c, m, m.command[1], "movie", "movie")

@bot1.on_message(filters.command("index_series") & filters.user(ADMIN_ID))
async def cmd_idx_series(c, m):
    if len(m.command) < 2: return await m.reply("Example: `/index_series @channel`")
    await universal_indexer(c, m, m.command[1], "series", "series")

@bot1.on_message(filters.command("index_full") & filters.user(ADMIN_ID))
async def cmd_idx_full(c, m):
    if len(m.command) < 2: return await m.reply("Example: `/index_full https://t.me/...`")
    await universal_indexer(c, m, m.command[1], "full", "all")

@bot1.on_message(filters.command("forward") & filters.user(ADMIN_ID))
async def cmd_fwd(c, m):
    if len(m.command) < 3: return await m.reply("Usage: `/forward <mode> <target_id>`")
    mode = m.command[1].lower()
    if mode not in DB_FILES: return await m.reply("❌ Mode must be: `movie`, `series`, `full`")
    await start_forwarding(m, mode, m.command[2], m.command[3] if len(m.command) > 3 else None)

@bot1.on_message(filters.command("stop") & filters.user(ADMIN_ID))
async def cmd_stop(c, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 **Force Stopping Tasks...**")

@bot1.on_callback_query(filters.regex("stop"))
async def cb_stop(c, q):
    if q.from_user.id == ADMIN_ID:
        global GLOBAL_TASK_RUNNING
        GLOBAL_TASK_RUNNING = False
        await q.answer("Stopping...")
        await q.message.edit("🛑 **Task Cancelled**")

# --- MAIN LOOP ---
async def main():
    print("🤖 Launching Bots...")
    try:
        await bot1.start()
        await bot2.start()
    except Exception as e:
        print(f"❌ Startup Error: {e}")
        return

    load_duplicates()
    
    # Auto-Notify Admin
    try:
        await bot1.send_message(ADMIN_ID, "✅ **Bot is Live!**\nRun `/sync` first to refresh cache.")
    except: pass
    
    print("🔥 System Ready!")
    await idle()
    await bot1.stop()
    await bot2.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
