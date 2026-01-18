from keep_alive import keep_alive
keep_alive()

import os, re, json, asyncio, time, random
from pyrogram import Client, filters, enums
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, 
    PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, RPCError, UsernameInvalid
)
from pyrogram.types import Message

# --- CONFIGURATION ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Client Setup (Memory Mode ON for max speed)
app = Client(
    "user_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    session_string=SESSION_STRING, 
    in_memory=True,
    no_updates=True # Updates band kar diye taki bot fast rahe
)

# --- GLOBAL SETTINGS (UPDATED) ---
GLOBAL_TASK_RUNNING = False
BATCH_SIZE = 250           # Har 250 messages ke baad break
BREAK_TIME = 30            # 30 Seconds ka break (Safety ke liye)
MIN_DELAY = 0.5            # Min sleep per message
MAX_DELAY = 1.5            # Max sleep per message (Randomize for safety)

# --- DATABASES ---
DB_FILES = {
    "movie_source": "db_movie_source.json",
    "movie_target": "db_movie_target.json",
    "full_source": "db_full_source.json",
    "full_target": "db_full_target.json",
    "history": "history_ids.txt"
}

# --- CACHE (Memory) ---
target_cache = {
    "unique_ids": set(),
    "name_size": set()
}

# --- REGEX PATTERNS ---
SERIES_REGEX = re.compile(r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE)
LINK_REGEX = re.compile(r"(?:https?://[^\s]+|t\.me/[^\s]+|@[\w]+)", re.IGNORECASE)
INVITE_LINK_REGEX = re.compile(r"(t\.me\/\+|joinchat\/|\?startinvite=|\?invite=)")

# --- HELPER FUNCTIONS ---

def only_admin(_, __, m: Message):
    return m.from_user and m.from_user.id == ADMIN_ID

def get_media_details(m: Message):
    """Media ka Name, Size aur ID nikalne ke liye smart function."""
    media = m.video or m.document or m.audio
    if not media: return None, 0, None
    
    file_name = getattr(media, 'file_name', None)
    file_size = getattr(media, 'file_size', 0)
    unique_id = getattr(media, 'file_unique_id', None)
    
    # Fallback agar file name nahi mila
    if not file_name:
        file_name = f"Unknown_File_{file_size}"
        
    return file_name, file_size, unique_id

async def resolve_chat(client, chat_ref):
    """
    Advanced Chat Resolver (From Reference Code)
    Handles: IDs, Usernames, Invite Links
    """
    chat_ref = str(chat_ref).strip()
    
    try:
        # 1. Check Numeric ID
        if chat_ref.lstrip('-').isdigit():
            return await client.get_chat(int(chat_ref))
            
        # 2. Check Invite Link
        if INVITE_LINK_REGEX.search(chat_ref):
            try:
                chat = await client.join_chat(chat_ref)
                return chat
            except UserAlreadyParticipant:
                pass # Already joined, proceed to get_chat
            except Exception as e:
                raise ValueError(f"Invite Link Error: {e}")

        # 3. Check Username / String
        return await client.get_chat(chat_ref)
        
    except (PeerIdInvalid, UsernameInvalid):
        raise ValueError("❌ Chat nahi mila. `/sync` chalao ya ID/Link check karo.")
    except Exception as e:
        raise ValueError(f"❌ Resolve Error: {e}")

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

    # 2. Load Target JSON
    if os.path.exists(db_file):
        try:
            with open(db_file, "r") as f:
                data = json.load(f)
                # Fast Set Update
                if "unique_ids" in data:
                    target_cache["unique_ids"].update(data["unique_ids"])
                if "compound_keys" in data:
                    target_cache["name_size"].update(data["compound_keys"])
        except Exception as e:
            print(f"Cache Load Error: {e}")

    print(f"✅ Cache Loaded: {len(target_cache['unique_ids'])} IDs ready.")

def save_history(unique_id):
    """Forward hone ke baad ID ko history me save karna (Anti-Dupe)."""
    if unique_id and unique_id not in target_cache["unique_ids"]:
        target_cache["unique_ids"].add(unique_id)
        with open(DB_FILES["history"], "a") as f:
            f.write(f"{unique_id}\n")

# --- CORE INDEXING ENGINE (Optimized) ---

async def indexing_engine(client, message, chat_ref, db_file, mode="all"):
    """
    Optimized Indexer using get_chat_history.
    Filters out text messages, keeps only Movies/Files.
    """
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply(f"🚀 **Fast Indexing Started...**\nTarget: `{chat_ref}`\nMode: `{mode.upper()}`\n\n_Scanning messages..._")
    
    try:
        chat = await resolve_chat(client, chat_ref)
        unique_ids_set = set() 
        name_size_set = set()  
        data_list = []
        
        count = 0
        found_count = 0
        last_update_time = time.time()
        
        # Iterating History (Oldest message first logic handle baad me hoga, yahan fetch latest se hota hai)
        async for m in client.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING:
                await status.edit("🛑 Task Stopped by User.")
                return

            # --- STRICT FILTERING (No Chat Messages) ---
            if not (m.video or m.document):
                continue

            file_name, file_size, unique_id = get_media_details(m)
            if not unique_id: continue

            # --- MODE FILTERING ---
            caption = m.caption or ""
            text_check = f"{file_name} {caption}"
            is_series = bool(SERIES_REGEX.search(text_check))
            
            if mode == "movie" and is_series: continue         # Skip Series
            # Full mode me sab kuch aayega (movies + series)

            # --- DATA COLLECTION ---
            # Agar Target Indexing hai (Duplicate check ke liye)
            if "target" in db_file:
                unique_ids_set.add(unique_id)
                if file_name and file_size > 0:
                    name_size_set.add(f"{file_name}-{file_size}")
            
            # Agar Source Indexing hai (Forwarding ke liye)
            else:
                data_list.append({
                    "msg_id": m.id,
                    "chat_id": chat.id,
                    "unique_id": unique_id,
                    "name": file_name,
                    "size": file_size
                })

            count += 1
            found_count += 1
            
            # Update Status every 5 seconds (Speed badhane ke liye har msg pe edit nahi karenge)
            if time.time() - last_update_time > 5:
                try:
                    await status.edit(f"⚡ **Indexing...**\nScanned: {count} Media Files\nFound: {found_count} Valid Items")
                    last_update_time = time.time()
                except: pass

        # --- SAVING DATA ---
        if "target" in db_file:
            final_data = {
                "unique_ids": list(unique_ids_set),
                "compound_keys": list(name_size_set)
            }
            with open(db_file, "w") as f:
                json.dump(final_data, f)
            msg = f"✅ **Target Indexing Complete!**\nChannel: `{chat.title}`\nTotal Media: {found_count}\nUnique IDs: {len(unique_ids_set)}"
        else:
            # Reverse list (Old to New) taki series/movies line se forward hon
            data_list.reverse()
            with open(db_file, "w") as f:
                json.dump(data_list, f, indent=2)
            msg = f"✅ **Source Indexing Complete!**\nChannel: `{chat.title}`\nTotal Media: {found_count}\nSaved to `{db_file}`."

        await status.edit(msg)

    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

# --- FORWARDING ENGINE (Smart & Safe) ---

async def forwarding_engine(message, source_db, target_db, destination_ref, limit=None, mode_copy=True):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    
    status = await message.reply("🔄 **Processing Database...**")
    
    # 1. Load Source
    if not os.path.exists(source_db):
        return await status.edit("❌ Source Index nahi mila! `/index` command chalao.")
    
    try:
        with open(source_db, "r") as f:
            source_data = json.load(f)
    except Exception as e:
        return await status.edit(f"❌ Database Error: {e}")

    # 2. Load Target (Duplicate Check)
    load_target_cache(target_db)
    
    # 3. Resolve Destination
    try:
        dest_chat = await resolve_chat(app, destination_ref)
    except Exception as e:
        return await status.edit(f"❌ Destination Error: {e}")

    # 4. Filter Duplicates (In Memory - Fast)
    final_list = []
    skipped_count = 0
    
    for item in source_data:
        u_id = item.get("unique_id")
        key = f"{item.get('name')}-{item.get('size')}"
        
        # Dual Check: Unique ID OR Name+Size
        if (u_id in target_cache["unique_ids"]) or (key in target_cache["name_size"]):
            skipped_count += 1
            continue
        final_list.append(item)
    
    if limit: final_list = final_list[:int(limit)]
    
    if not final_list:
        return await status.edit(f"✅ **Task Finished!**\nKoi nayi movie nahi mili.\nSkipped Duplicates: {skipped_count}")

    await status.edit(f"🚀 **Forwarding Started!**\nTarget: `{dest_chat.title}`\nQueue: {len(final_list)}\nSkipped Duplicates: {skipped_count}\n\n_Safe Mode On (Random Delays)_")

    # 5. Execution Loop
    success = 0
    consecutive_batch_count = 0
    
    for i, item in enumerate(final_list):
        if not GLOBAL_TASK_RUNNING: break
        
        try:
            # Mode Check: Copy (No Tag) or Forward (With Tag)
            if mode_copy:
                await app.copy_message(dest_chat.id, item['chat_id'], item['msg_id'])
            else:
                await app.forward_messages(dest_chat.id, item['chat_id'], item['msg_id'])
            
            # Save ID immediately
            save_history(item.get("unique_id"))
            success += 1
            consecutive_batch_count += 1
            
            # Status Update (Thoda interval pe)
            if i % 20 == 0:
                try:
                    await status.edit(f"🚀 **Forwarding...**\nDone: {success} / {len(final_list)}\nSkipped: {skipped_count}")
                except: pass
            
            # --- BREAK LOGIC (UPDATED) ---
            if consecutive_batch_count >= BATCH_SIZE:
                await status.edit(f"☕ **Break Time!**\n{BATCH_SIZE} files forwarded.\nSleeping for {BREAK_TIME} seconds (Anti-Ban)...")
                await asyncio.sleep(BREAK_TIME)
                consecutive_batch_count = 0 # Reset counter
                await status.edit(f"🚀 **Resuming...**")
            else:
                # Random Jitter (Safety)
                await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        except FloodWait as e:
            await status.edit(f"⏳ **FloodWait Hit!**\nTelegram ne roka hai.\nSleeping {e.value}s...")
            await asyncio.sleep(e.value + 5)
        except MessageIdInvalid:
            print(f"Message {item['msg_id']} deleted from source.")
        except Exception as e:
            print(f"Forward Error: {e}")

    GLOBAL_TASK_RUNNING = False
    await status.edit(f"✅ **Completed Successfully!**\nForwarded: {success}\nDuplicates Skipped: {skipped_count}\nTarget: `{dest_chat.title}`")

# --- COMMANDS ---

@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_msg(_, m):
    txt = (
        "🤖 **Pro Movie Forwarder Bot** (Upgraded)\n\n"
        "**⚡ Indexing Commands:**\n"
        "`/index <channel>` - Index Movies (Source)\n"
        "`/index_full <channel>` - Index Everything (Source)\n"
        "`/index_target <channel>` - Index Target (For Anti-Duplicate)\n"
        "`/index_target_full <channel>` - Index Full Target\n\n"
        "**🚀 Forwarding Commands:**\n"
        "`/forward_movie <target>` - Movies forward karega.\n"
        "`/forward_full <target>` - Sab kuch forward karega.\n"
        "_Example:_ `/forward_movie -100123456`\n\n"
        "**🛠 Utility:**\n"
        "`/stats` - Database stats dekhein.\n"
        "`/clean_dupes <channel>` - Channel se duplicates delete karein.\n"
        "`/sync` - Agar chat ID error aaye.\n"
        "`/stop` - Sab rokne ke liye."
    )
    await m.reply(txt)

@app.on_message(filters.command("stats") & filters.create(only_admin))
async def stats_cmd(_, m):
    """Shows statistics of indexed files."""
    msg_text = "📊 **Database Statistics**\n\n"
    
    # Check Source DBs
    if os.path.exists(DB_FILES["movie_source"]):
        with open(DB_FILES["movie_source"]) as f:
            msg_text += f"🎬 Movies Indexed: `{len(json.load(f))}`\n"
    else:
        msg_text += "🎬 Movies Indexed: `0`\n"

    if os.path.exists(DB_FILES["full_source"]):
        with open(DB_FILES["full_source"]) as f:
            msg_text += f"📂 Full Media Indexed: `{len(json.load(f))}`\n"
    else:
        msg_text += "📂 Full Media Indexed: `0`\n"

    # Check Target DBs
    t_count = 0
    if os.path.exists(DB_FILES["movie_target"]):
        with open(DB_FILES["movie_target"]) as f:
            t_count = len(json.load(f).get("unique_ids", []))
    msg_text += f"🛡 Target (Movies) Cache: `{t_count}`\n"
    
    # Check History
    h_count = 0
    if os.path.exists(DB_FILES["history"]):
        with open(DB_FILES["history"]) as f:
            h_count = len(f.readlines())
    msg_text += f"📜 Total History (Forwarded): `{h_count}`\n\n"
    
    msg_text += f"⚙️ **Settings:**\nBatch Size: `{BATCH_SIZE}`\nBreak Time: `{BREAK_TIME}s`"
    
    await m.reply(msg_text)

@app.on_message(filters.command("sync") & filters.create(only_admin))
async def sync_cmd(_, m):
    msg = await m.reply("♻️ **Syncing Dialogs...**")
    try:
        count = 0
        async for dialog in app.get_dialogs():
            count += 1
        await msg.edit(f"✅ Synced {count} chats successfully! Now try indexing.")
    except Exception as e:
        await msg.edit(f"❌ Sync Error: {e}")

@app.on_message(filters.command("clean_dupes") & filters.create(only_admin))
async def clean_dupes_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/clean_dupes @channel`")
    
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = True
    status = await m.reply("♻️ **Scanning for Duplicates...**\n(Name + Size check kar raha hu)")
    
    try:
        chat = await resolve_chat(c, m.command[1])
        seen_keys = set()
        delete_list = []
        scanned = 0
        
        async for msg in c.get_chat_history(chat.id):
            if not GLOBAL_TASK_RUNNING: break
            if not (msg.video or msg.document): continue
            
            scanned += 1
            fname, fsize, _ = get_media_details(msg)
            if not fname: continue
            
            key = f"{fname}-{fsize}"
            
            if key in seen_keys:
                delete_list.append(msg.id)
            else:
                seen_keys.add(key)
            
            if scanned % 500 == 0:
                await status.edit(f"♻️ Scanning: {scanned}\nFound Dupes: {len(delete_list)}")
        
        if not delete_list:
            return await status.edit("✅ No duplicates found.")
            
        await status.edit(f"🗑️ Found {len(delete_list)} duplicates.\nDeleting in batches of 100...")
        
        # Safe Batch Delete
        for i in range(0, len(delete_list), 100):
            if not GLOBAL_TASK_RUNNING: break
            batch = delete_list[i:i+100]
            try:
                await c.delete_messages(chat.id, batch)
                await asyncio.sleep(2)
            except Exception as e:
                print(f"Delete Error: {e}")
            
        await status.edit(f"✅ Cleanup Complete! Deleted {len(delete_list)} messages.")
        
    except Exception as e:
        await status.edit(f"Error: {e}")
    finally:
        GLOBAL_TASK_RUNNING = False

# --- INDEX COMMANDS ---
@app.on_message(filters.command("index") & filters.create(only_admin))
async def cmd_idx_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index -100xxxx`")
    await indexing_engine(c, m, m.command[1], DB_FILES["movie_source"], mode="movie")

@app.on_message(filters.command("index_target") & filters.create(only_admin))
async def cmd_idx_tgt_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target -100xxxx`")
    await indexing_engine(c, m, m.command[1], DB_FILES["movie_target"], mode="target")

@app.on_message(filters.command("index_full") & filters.create(only_admin))
async def cmd_idx_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_full -100xxxx`")
    await indexing_engine(c, m, m.command[1], DB_FILES["full_source"], mode="all")

@app.on_message(filters.command("index_target_full") & filters.create(only_admin))
async def cmd_idx_tgt_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/index_target_full -100xxxx`")
    await indexing_engine(c, m, m.command[1], DB_FILES["full_target"], mode="target")

# --- FORWARD COMMANDS ---
@app.on_message(filters.command("forward_movie") & filters.create(only_admin))
async def cmd_fwd_mov(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/forward_movie <target_id> [limit]`")
    limit = m.command[2] if len(m.command) > 2 else None
    await forwarding_engine(m, DB_FILES["movie_source"], DB_FILES["movie_target"], m.command[1], limit)

@app.on_message(filters.command("forward_full") & filters.create(only_admin))
async def cmd_fwd_full(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/forward_full <target_id> [limit]`")
    limit = m.command[2] if len(m.command) > 2 else None
    await forwarding_engine(m, DB_FILES["full_source"], DB_FILES["full_target"], m.command[1], limit)

@app.on_message(filters.command("stop") & filters.create(only_admin))
async def stop_cmd(_, m):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await m.reply("🛑 **All tasks stopped!**")

# --- MAIN RUNNER ---
if __name__ == "__main__":
    print("🚀 Pro Bot Started... (Waiting for commands)")
    app.run()
