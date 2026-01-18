from keep_alive import keep_alive
# Web server start
keep_alive()

import os, re, json, asyncio, sys
from pyrogram import Client, filters, idle
from pyrogram.errors import (
    FloodWait, PeerIdInvalid, UserAlreadyParticipant, UsernameInvalid
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- CONFIGURATION CHECK ---
print("⚙️ Loading Configuration...")

try:
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH")
    SESSION1 = os.getenv("SESSION_STRING")       # Boss Account
    SESSION2 = os.getenv("SESSION_STRING_2")     # Worker Account
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))   # Admin ID
    
    # Validation
    if not API_ID or not API_HASH:
        print("❌ Error: API_ID ya API_HASH missing hai.")
        sys.exit(1)
    if not SESSION1:
        print("❌ Error: SESSION_STRING (Boss) missing hai. Render Env Vars check karein.")
        sys.exit(1)
    if not SESSION2:
        print("⚠️ Warning: SESSION_STRING_2 (Worker) missing hai. Bot 2 fail ho sakta hai.")
    if ADMIN_ID == 0:
        print("❌ Error: ADMIN_ID missing hai.")
        sys.exit(1)

except ValueError:
    print("❌ Error: API_ID aur ADMIN_ID sirf numbers hone chahiye.")
    sys.exit(1)

# --- INITIALIZE DUAL CLIENTS ---
# in_memory=True zaruri hai taki Render par file permission ka issue na aaye
bot1 = Client("boss_session", api_id=API_ID, api_hash=API_HASH, session_string=SESSION1, in_memory=True)
bot2 = Client("worker_session", api_id=API_ID, api_hash=API_HASH, session_string=SESSION2, in_memory=True)

# --- GLOBAL STATE ---
GLOBAL_TASK_RUNNING = False
PER_MSG_DELAY = 1.0       
BATCH_SIZE = 100          
BREAK_TIME = 30           

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

# --- CACHE ---
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

def load_duplicates():
    global processed_unique_ids, processed_name_size
    processed_unique_ids.clear()
    processed_name_size.clear()
    
    if os.path.exists(DB_FILES["history"]):
        with open(DB_FILES["history"], "r") as f:
            for line in f:
                processed_unique_ids.add(line.strip())

    for key in ["movie_target", "series_target", "full_target"]:
        if os.path.exists(DB_FILES[key]):
            try:
                with open(DB_FILES[key], "r") as f:
                    data = json.load(f)
                    processed_unique_ids.update(data.get("unique_ids", []))
                    processed_name_size.update(data.get("compound_keys", []))
            except: pass
    print(f"✅ Loaded {len(processed_unique_ids)} Unique IDs.")

def save_forwarded(unique_id, name, size):
    if unique_id:
        processed_unique_ids.add(unique_id)
        with open(DB_FILES["history"], "a") as f:
            f.write(f"{unique_id}\n")
    if name and size:
        processed_name_size.add(f"{name}-{size}")

def get_media_details(m):
    media = m.video or m.document or m.audio
    if not media: return None, None, None
    return getattr(media, 'file_name', None), getattr(media, 'file_size', 0), getattr(media, 'file_unique_id', None)

async def resolve_chat(client, chat_ref):
    try:
        return await client.get_chat(chat_ref)
    except (PeerIdInvalid, UsernameInvalid):
        if "t.me" in str(chat_ref):
            try:
                return await client.join_chat(chat_ref)
            except UserAlreadyParticipant:
                pass
        raise ValueError(f"Chat access denied: {chat_ref}")

async def join_sync(client, chat_ref):
    try:
        await client.join_chat(chat_ref)
        return True
    except UserAlreadyParticipant:
        return True
    except Exception as e:
        print(f"Join Error: {e}")
        return False

# --- INDEXING ENGINES ---

async def generic_indexer(client, message, chat_ref, db_file, mode="all"):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply(f"⏳ **Smart Indexing Started**\nMode: `{mode.upper()}`")
    
    try:
        chat = await resolve_chat(client, chat_ref)
        data_list = []
        found_count = 0
        
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING:
                await status.edit("🛑 Task Stopped.")
                return

            if not (m.video or m.document): continue

            file_name, file_size, unique_id = get_media_details(m)
            if not unique_id: continue

            caption = m.caption or ""
            text_check = f"{file_name} {caption}"
            is_series = bool(SERIES_REGEX.search(text_check))
            
            if mode == "movie" and is_series: continue
            elif mode == "series" and not is_series: continue
            elif mode == "bad" and not BAD_QUALITY_REGEX.search(text_check): continue
            
            meta = {}
            if mode == "series":
                match = EPISODE_INFO_REGEX.search(text_check)
                if match:
                    meta = {"name": match.group(1).strip(), "season": int(match.group(2)), "episode": int(match.group(3))}
                else:
                    meta = {"name": file_name, "season": 1, "episode": 999}

            data_list.append({
                "msg_id": m.id,
                "chat_id": chat.id,
                "unique_id": unique_id,
                "name": file_name,
                "size": file_size,
                "meta": meta
            })
            found_count += 1
            if found_count % 1000 == 0:
                print(f"Indexed: {found_count}")

        if mode in ["all", "movie", "bad"]: data_list.reverse()
        elif mode == "series": data_list.sort(key=lambda x: (x['meta'].get('name', ''), x['meta'].get('season', 0), x['meta'].get('episode', 0)))

        with open(db_file, "w", encoding="utf-8") as f:
            json.dump(data_list, f, indent=2)

        await status.edit(f"✅ **Saved!** Total: {found_count}")

    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

async def target_indexer(client, message, chat_ref, db_file):
    status = await message.reply("⏳ **Target Syncing...**")
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
        
        load_duplicates()
        await status.edit(f"✅ **Target Synced!** IDs: {len(u_ids)}")
    except Exception as e:
        await status.edit(f"❌ Error: {e}")

# --- DUAL-CORE FORWARDING ---

async def forward_worker(client, target_id, batch, worker_name, status_msg, progress_dict):
    local_count = 0
    for item in batch:
        if not GLOBAL_TASK_RUNNING: break
        
        msg_id = item['msg_id']
        src_id = item['chat_id']
        unique_id = item.get('unique_id')
        name = item.get('name')
        size = item.get('size')
        
        compound_key = f"{name}-{size}"
        if unique_id in processed_unique_ids or compound_key in processed_name_size:
            continue

        try:
            await client.forward_messages(target_id, src_id, msg_id)
            save_forwarded(unique_id, name, size)
            local_count += 1
            progress_dict[worker_name] += 1
            
            if local_count % BATCH_SIZE == 0: await asyncio.sleep(BREAK_TIME)
            else: await asyncio.sleep(PER_MSG_DELAY)

        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            print(f"[{worker_name}] Error: {e}")

async def dual_forwarder(message, db_file, target_ref, limit=None):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("🚀 **Preparing Dual-Core Forwarding...**")
    
    if not os.path.exists(db_file):
        await status.edit("❌ DB missing.")
        GLOBAL_TASK_RUNNING = False
        return

    with open(db_file, "r") as f:
        data = json.load(f)

    load_duplicates()
    clean_data = []
    for item in data:
        u_id = item.get('unique_id')
        key = f"{item.get('name')}-{item.get('size')}"
        if u_id not in processed_unique_ids and key not in processed_name_size:
            clean_data.append(item)
    
    if limit: clean_data = clean_data[:int(limit)]
    total_items = len(clean_data)
    
    if total_items == 0:
        await status.edit("✅ No new files.")
        GLOBAL_TASK_RUNNING = False
        return

    try:
        tgt = await resolve_chat(bot1, target_ref)
        tgt_id = tgt.id
        await join_sync(bot2, target_ref)
    except Exception as e:
        await status.edit(f"❌ Target Error: {e}")
        GLOBAL_TASK_RUNNING = False
        return

    list_1 = clean_data[0::2]
    list_2 = clean_data[1::2]

    await status.edit(f"⚡ **Started!** Boss: {len(list_1)} | Worker: {len(list_2)}")
    
    progress = {"Boss": 0, "Worker": 0}
    
    async def update_ui():
        while GLOBAL_TASK_RUNNING:
            done = progress["Boss"] + progress["Worker"]
            if done >= total_items: break
            try:
                await status.edit(
                    f"⚡ **Progress:** `{done}/{total_items}`\nBoss: {progress['Boss']} | Worker: {progress['Worker']}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 STOP", callback_data="stop")]])
                )
            except: pass
            await asyncio.sleep(5)

    ui_task = asyncio.create_task(update_ui())
    task1 = asyncio.create_task(forward_worker(bot1, tgt_id, list_1, "Boss", status, progress))
    task2 = asyncio.create_task(forward_worker(bot2, tgt_id, list_2, "Worker", status, progress))
    
    await asyncio.gather(task1, task2)
    GLOBAL_TASK_RUNNING = False
    await ui_task
    await status.edit("✅ **Completed!**")

# --- COMMANDS ---

@bot1.on_message(filters.command("start") & filters.user(ADMIN_ID))
async def start_msg(_, m):
    await m.reply(
        "🔥 **DUAL-CORE BOT ONLINE**\n"
        "`/sync @channel` - Join & Sync\n"
        "`/index @channel` - Index Movie\n"
        "`/index_target @channel` - Target Movie\n"
        "`/index_full @channel` - Index All\n"
        "`/index_target_full @channel` - Target All\n"
        "`/forward movie @target`\n"
        "`/forward full @target`\n"
        "`/stop` - Stop"
    )

@bot1.on_message(filters.command("stop") & filters.user(ADMIN_ID))
async def stop_handler(_, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 Stopping...")

@bot1.on_callback_query(filters.regex("stop"))
async def stop_cb(_, q):
    global GLOBAL_TASK_RUNNING
    if q.from_user.id != ADMIN_ID: return
    GLOBAL_TASK_RUNNING = False
    await q.answer()
    await q.message.edit("🛑 **STOPPED**")

@bot1.on_message(filters.command("ping") & filters.user(ADMIN_ID))
async def ping_handler(_, m):
    try:
        me1 = await bot1.get_me()
        me2 = await bot2.get_me()
        await m.reply(f"✅ Bot1: `{me1.first_name}`\n✅ Bot2: `{me2.first_name}`")
    except Exception as e:
        await m.reply(f"⚠️ Error: {e}")

@bot1.on_message(filters.command("sync") & filters.user(ADMIN_ID))
async def sync_command(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/sync @channel`")
    chat_ref = m.command[1]
    status = await m.reply("♻️ **Syncing...**")
    try:
        await join_sync(bot1, chat_ref)
        await join_sync(bot2, chat_ref)
        await target_indexer(c, m, chat_ref, DB_FILES["full_target"])
    except Exception as e:
        await status.edit(f"❌ Failed: {e}")

@bot1.on_message(filters.command("index") & filters.user(ADMIN_ID))
async def cmd_idx_movie(c, m): await generic_indexer(c, m, m.command[1], DB_FILES["movie_index"], mode="movie")

@bot1.on_message(filters.command("index_target") & filters.user(ADMIN_ID))
async def cmd_idx_tgt_movie(c, m): await target_indexer(c, m, m.command[1], DB_FILES["movie_target"])

@bot1.on_message(filters.command("index_full") & filters.user(ADMIN_ID))
async def cmd_idx_full(c, m): await generic_indexer(c, m, m.command[1], DB_FILES["full_index"], mode="all")

@bot1.on_message(filters.command("index_target_full") & filters.user(ADMIN_ID))
async def cmd_idx_tgt_full(c, m): await target_indexer(c, m, m.command[1], DB_FILES["full_target"])

@bot1.on_message(filters.command("forward") & filters.user(ADMIN_ID))
async def cmd_forward(_, m):
    if len(m.command) < 3: return await m.reply("Usage: `/forward <type> <target>`")
    db_map = {"movie": DB_FILES["movie_index"], "full": DB_FILES["full_index"]}
    db_type = m.command[1].lower()
    if db_type not in db_map: return await m.reply("❌ Use `movie` or `full`")
    await dual_forwarder(m, db_map[db_type], m.command[2], m.command[3] if len(m.command) > 3 else None)

# --- MAIN EXECUTION ---
async def main():
    print("🤖 Starting Bots...")
    # Error handling during startup
    try:
        await bot1.start()
    except Exception as e:
        print(f"❌ BOSS BOT FAILED: {e}")
        print("💡 Check SESSION_STRING in Render.")
        sys.exit(1)

    try:
        await bot2.start()
    except Exception as e:
        print(f"⚠️ WORKER BOT FAILED: {e}")
        print("💡 Continuing with Boss only...")

    # Notify Admin
    try:
        await bot1.send_message(ADMIN_ID, "✅ **Bot Restarted!**\nSend `/start`")
    except:
        print("⚠️ Failed to send start message.")

    load_duplicates()
    print("🔥 Ready!")
    await idle()
    await bot1.stop()
    await bot2.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
