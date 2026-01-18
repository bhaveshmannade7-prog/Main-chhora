from keep_alive import keep_alive
keep_alive()

import os, re, json, asyncio, time
from pyrogram import Client, filters, enums
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, 
    PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, 
    RPCError, UsernameInvalid, ChannelPrivate
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- CONFIGURATION ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Client Setup (Memory Mode ON for speed)
app = Client("user_bot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING, in_memory=True)

# --- GLOBAL SETTINGS ---
GLOBAL_TASK_RUNNING = False
PER_MSG_DELAY = 0.5        # Fast Speed
BATCH_SIZE = 200           # Break lene se pehle messages
BREAK_TIME = 10            # Break duration

# --- DATABASES ---
DB_FILES = {
    "movie_source": "db_movie_source.json",
    "movie_target": "db_movie_target.json",
    "series_source": "db_series_source.json",
    "series_target": "db_series_target.json",
    "full_source": "db_full_source.json",
    "full_target": "db_full_target.json",
    "bad_quality": "db_bad_quality.json",
    "history": "history_ids.txt"
}

# --- CACHE (Memory) ---
# Isme hum target channel ka data load karenge taaki checking fast ho
target_cache = {
    "unique_ids": set(),
    "name_size": set()
}

# --- REGEX PATTERNS ---
SERIES_REGEX = re.compile(r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE)
EPISODE_INFO = re.compile(r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(?:E|Ep|Episode)\s*(\d{1,3})", re.IGNORECASE | re.DOTALL)
BAD_QUALITY_REGEX = re.compile(r"\b(cam|camrip|hdcam|ts|telesync|tc|pre-dvdrip|scr|screener|bad audio)\b", re.IGNORECASE)
LINK_REGEX = re.compile(r"(?:https?://[^\s]+|t\.me/[^\s]+|@[\w]+)", re.IGNORECASE)

# --- HELPER FUNCTIONS ---

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def get_media_details(m):
    """Media ka Name, Size aur ID nikalne ke liye smart function."""
    media = m.video or m.document or m.audio
    if not media: return None, 0, None
    return getattr(media, 'file_name', "Unknown"), getattr(media, 'file_size', 0), getattr(media, 'file_unique_id', None)

async def resolve_chat(client, chat_ref):
    """ID (-100..) ya Username (@..) dono ko handle karega."""
    try:
        # Agar numeric ID string me hai
        if str(chat_ref).lstrip('-').isdigit():
            return await client.get_chat(int(chat_ref))
        # Username handling
        return await client.get_chat(chat_ref)
    except (PeerIdInvalid, UsernameInvalid):
        # Agar bot ne chat join nahi kiya hai
        if "t.me" in str(chat_ref):
            try: return await client.join_chat(chat_ref)
            except: pass
        raise ValueError("❌ Chat nahi mila. `/sync` command chalao ya ID check karo.")

def load_target_cache(db_file):
    """Target DB se duplicate data memory me load karna."""
    global target_cache
    target_cache["unique_ids"].clear()
    target_cache["name_size"].clear()
    
    # 1. Load History File (Permanent Logs)
    if os.path.exists(DB_FILES["history"]):
        with open(DB_FILES["history"], "r") as f:
            for line in f:
                target_cache["unique_ids"].add(line.strip())

    # 2. Load Target JSON (Current Scan)
    if os.path.exists(db_file):
        try:
            with open(db_file, "r") as f:
                data = json.load(f)
                # Purana format support
                if isinstance(data, dict):
                    target_cache["unique_ids"].update(data.get("unique_ids", []))
                    target_cache["name_size"].update(data.get("compound_keys", []))
                # Naya List format support
                elif isinstance(data, list):
                    for item in data:
                        if item.get("unique_id"): target_cache["unique_ids"].add(item["unique_id"])
                        if item.get("name") and item.get("size"):
                            target_cache["name_size"].add(f"{item['name']}-{item['size']}")
        except Exception as e:
            print(f"Cache Load Error: {e}")

    print(f"✅ Loaded Duplicates: {len(target_cache['unique_ids'])} IDs")

def save_history(unique_id):
    """Forward hone ke baad ID ko history me save karna."""
    if unique_id:
        target_cache["unique_ids"].add(unique_id)
        with open(DB_FILES["history"], "a") as f:
            f.write(f"{unique_id}\n")

# --- CORE INDEXING ENGINE (Get Chat History) ---

async def indexing_engine(client, message, chat_ref, db_file, mode="all"):
    """
    Ek Powerful Indexer jo Source aur Target dono ke liye kaam karega.
    Mode: 'movie', 'series', 'full'
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply(f"⏳ **Indexing Started...**\nTarget: `{chat_ref}`\nMode: `{mode.upper()}`\nMethod: `get_chat_history` (100% Accurate)")
    
    try:
        chat = await resolve_chat(client, chat_ref)
        data_list = []
        unique_ids_set = set() # For Target DB structure
        name_size_set = set()  # For Target DB structure
        
        count = 0
        video_count = 0
        
        # 'get_chat_history' sabse accurate hota hai
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING:
                await status.edit("🛑 Task Stopped by User.")
                return

            if not (m.video or m.document or m.audio):
                continue
            
            file_name, file_size, unique_id = get_media_details(m)
            if not unique_id: continue

            # Filters Logic
            caption = m.caption or ""
            text_check = f"{file_name} {caption}"
            is_series = bool(SERIES_REGEX.search(text_check))
            
            if mode == "movie" and is_series: continue         # Skip Series
            if mode == "series" and not is_series: continue    # Skip Movies
            
            # Data Structure Check
            # Agar hum Target Index kar rahe hain, humein bas ID aur Name chahiye checking ke liye
            if "target" in db_file:
                unique_ids_set.add(unique_id)
                if file_name and file_size:
                    name_size_set.add(f"{file_name}-{file_size}")
            else:
                # Source Indexing me humein pura data chahiye forward karne ke liye
                # Series Metadata Extraction
                meta = {}
                if mode == "series":
                    match = EPISODE_INFO.search(text_check)
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

            count += 1
            if count % 500 == 0:
                await status.edit(f"⚡ Scanning `{chat.title}`...\nScanned: {count} files")

        # Saving Logic
        if "target" in db_file:
            # Target DB Format: Sets of IDs (Fast Lookup)
            final_data = {
                "unique_ids": list(unique_ids_set),
                "compound_keys": list(name_size_set)
            }
            with open(db_file, "w") as f:
                json.dump(final_data, f)
            msg = f"✅ **Target Indexing Complete!**\nFound: {len(unique_ids_set)} unique files.\nAb Source index karke Forward karo, duplicates skip honge."
        else:
            # Source DB Format: List of Messages (For Forwarding)
            # Reverse list (Oldest to Newest)
            if mode != "series": data_list.reverse()
            # Sort Series
            if mode == "series":
                data_list.sort(key=lambda x: (x['meta'].get('name', ''), x['meta'].get('season', 0), x['meta'].get('episode', 0)))

            with open(db_file, "w") as f:
                json.dump(data_list, f, indent=2)
            msg = f"✅ **Source Indexing Complete!**\nFound: {len(data_list)} files.\nSaved to `{db_file}`."

        await status.edit(msg)

    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

# --- FORWARDING ENGINE (With Anti-Duplicate) ---

async def forwarding_engine(message, source_db, target_db, destination_ref, limit=None, mode_copy=True):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("🚀 **Preparing Forwarder...**")
    
    # 1. Load Source Data
    if not os.path.exists(source_db):
        return await status.edit("❌ Source Index Missing! Pehle `/index` command chalao.")
    
    with open(source_db, "r") as f:
        source_data = json.load(f)

    # 2. Load Target Data (Duplicates)
    # Yeh step sabse zaruri hai duplicate rokne ke liye
    load_target_cache(target_db)
    
    # 3. Resolve Destination
    try:
        dest_chat = await resolve_chat(app, destination_ref)
    except Exception as e:
        return await status.edit(f"❌ Destination Error: {e}")

    # 4. Filter Logic (Skip Duplicates)
    final_list = []
    skipped_count = 0
    
    for item in source_data:
        u_id = item.get("unique_id")
        key = f"{item.get('name')}-{item.get('size')}"
        
        # Check: Kya ye Target DB me hai?
        if u_id in target_cache["unique_ids"] or key in target_cache["name_size"]:
            skipped_count += 1
            continue
        final_list.append(item)
    
    if limit: final_list = final_list[:int(limit)]
    
    if not final_list:
        return await status.edit(f"✅ **Sab kuch Up-to-Date hai!**\nSource ke saare files Target me pehle se the.\nSkipped: {skipped_count}")

    await status.edit(f"⚡ **Forwarding Started**\nTotal New: {len(final_list)}\nSkipped (Duplicates): {skipped_count}\nTarget: `{dest_chat.title}`")

    # 5. Execution Loop
    success = 0
    for i, item in enumerate(final_list):
        if not GLOBAL_TASK_RUNNING: break
        
        try:
            if mode_copy:
                await app.copy_message(dest_chat.id, item['chat_id'], item['msg_id'])
            else:
                await app.forward_messages(dest_chat.id, item['chat_id'], item['msg_id'])
            
            # Save to history immediately
            save_history(item.get("unique_id"))
            success += 1
            
            # UI Update
            if i % 20 == 0:
                await status.edit(f"🚀 **Forwarding...**\nProgress: {success} / {len(final_list)}\nSkipped: {skipped_count}")
            
            # Batch Break
            if i > 0 and i % BATCH_SIZE == 0:
                await status.edit(f"☕ Taking a break for {BREAK_TIME}s...")
                await asyncio.sleep(BREAK_TIME)
            else:
                await asyncio.sleep(PER_MSG_DELAY)

        except FloodWait as e:
            await status.edit(f"⏳ FloodWait: Sleeping {e.value}s...")
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            print(f"Forward Error: {e}")

    GLOBAL_TASK_RUNNING = False
    await status.edit(f"✅ **Task Completed!**\nForwarded: {success}\nDuplicates Skipped: {skipped_count}")

# --- COMMANDS ---

@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_msg(_, m):
    txt = (
        "🤖 **Advanced Duplicate-Remover Bot**\n\n"
        "**STEP 1: Target Indexing (Zaruri Hai)**\n"
        "`/index_target <id/link>` - Target channel scan karein (Dupes bachane ke liye).\n"
        "`/index_target_full <id/link>` - Full scan Target.\n\n"
        "**STEP 2: Source Indexing**\n"
        "`/index <id/link>` - Movies Indexing.\n"
        "`/index_full <id/link>` - Full Channel Indexing.\n"
        "`/index_series <id/link>` - Series Indexing.\n\n"
        "**STEP 3: Forwarding (Smart)**\n"
        "`/forward <mode> <target_id>`\n"
        "Modes: `movie`, `full`, `series`\n"
        "Ex: `/forward full -100123456`\n\n"
        "**Extras:**\n"
        "`/sync` - Fix Chat ID errors.\n"
        "`/clean_dupes <id>` - Delete existing duplicates.\n"
        "`/stop` - Stop processes."
    )
    await m.reply(txt)

@app.on_message(filters.command("sync") & filters.create(only_admin))
async def sync_cmd(_, m):
    """Global Sync to fix PeerIdInvalid errors."""
    msg = await m.reply("♻️ **Syncing Dialogs...**")
    try:
        count = 0
        async for dialog in app.get_dialogs():
            count += 1
        await msg.edit(f"✅ Synced {count} chats successfully!")
    except Exception as e:
        await msg.edit(f"❌ Sync Error: {e}")

# --- INDEX COMMANDS ---

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

@app.on_message(filters.command("index_series") & filters.create(only_admin))
async def cmd_idx_series(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_series @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["series_source"], mode="series")

@app.on_message(filters.command("index_target_series") & filters.create(only_admin))
async def cmd_idx_tgt_series(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target_series @channel`")
    await indexing_engine(c, m, m.command[1], DB_FILES["series_target"], mode="target")

# --- FORWARD COMMAND ---

@app.on_message(filters.command("forward") & filters.create(only_admin))
async def cmd_forward(c, m):
    # /forward full -100123456
    if len(m.command) < 3:
        return await m.reply("Usage: `/forward <mode> <target_id>`\nModes: `movie`, `full`, `series`")
    
    mode = m.command[1].lower()
    target_ref = m.command[2]
    limit = m.command[3] if len(m.command) > 3 else None
    
    config_map = {
        "movie": (DB_FILES["movie_source"], DB_FILES["movie_target"]),
        "full": (DB_FILES["full_source"], DB_FILES["full_target"]),
        "series": (DB_FILES["series_source"], DB_FILES["series_target"]),
    }
    
    if mode not in config_map:
        return await m.reply("❌ Invalid Mode. Use `movie`, `full`, or `series`.")
    
    source_db, target_db = config_map[mode]
    await forwarding_engine(m, source_db, target_db, target_ref, limit, mode_copy=True)

# --- UTILS ---

@app.on_message(filters.command("stop") & filters.create(only_admin))
async def stop_cmd(_, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 Stopping all tasks...")

@app.on_message(filters.command("clean_dupes") & filters.create(only_admin))
async def clean_dupes_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/clean_dupes @channel`")
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    status = await m.reply("♻️ Scanning for duplicates inside the channel...")
    
    try:
        chat = await resolve_chat(c, m.command[1])
        seen_keys = set()
        delete_list = []
        
        async for msg in c.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING: break
            if not (msg.video or msg.document): continue
            
            fname, fsize, _ = get_media_details(msg)
            if not fname: continue
            
            key = f"{fname}-{fsize}"
            if key in seen_keys:
                delete_list.append(msg.id)
            else:
                seen_keys.add(key)
        
        if not delete_list:
            return await status.edit("✅ No duplicates found inside this channel.")
            
        await status.edit(f"🗑️ Found {len(delete_list)} duplicates. Deleting...")
        
        # Batch Delete
        for i in range(0, len(delete_list), 100):
            await c.delete_messages(chat.id, delete_list[i:i+100])
            await asyncio.sleep(2)
            
        await status.edit(f"✅ Deleted {len(delete_list)} duplicate files.")
        
    except Exception as e:
        await status.edit(f"Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

# --- MAIN RUNNER ---
if __name__ == "__main__":
    print("🤖 Bot Started...")
    app.run()
