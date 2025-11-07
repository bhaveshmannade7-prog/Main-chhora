from keep_alive import keep_alive
keep_alive()

import os, time, re, json
import asyncio
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, RPCError
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Session via string (Pyrogram v2)
app = Client("user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

# --- Runtime state ---
target_channel = None
limit_messages = None
forwarded_count = 0
is_forwarding = False
mode_copy = True
# Har movie ke baad 0.5 sec ka pause stability ke liye
PER_MSG_DELAY = 0.5 

# --- Database Files ---
DUPLICATE_DB_FILE = "forwarded_unique_ids.txt"
INDEX_DB_FILE = "movie_database.json"
TARGET_INDEX_DB_FILE = "target_index.json"

forwarded_unique_ids = set()

def get_default_index():
    return {
        "source_channel_id": None,
        "source_channel_name": None,
        "movies": {}
    }
movie_index = get_default_index()


# --- Database Load/Save Functions ---

def load_forwarded_ids():
    global forwarded_unique_ids
    forwarded_unique_ids = set()
    if os.path.exists(DUPLICATE_DB_FILE):
        try:
            with open(DUPLICATE_DB_FILE, "r") as f:
                for line in f:
                    forwarded_unique_ids.add(line.strip())
        except Exception as e:
            print(f"[DB ERR] loading duplicate DB: {e}")
            
    # Target index se bhi IDs load karo
    if os.path.exists(TARGET_INDEX_DB_FILE):
        try:
            with open(TARGET_INDEX_DB_FILE, "r") as f:
                target_ids = json.load(f)
                forwarded_unique_ids.update(target_ids)
            print(f"Loaded {len(target_ids)} IDs from target index.")
        except Exception as e:
            print(f"[DB ERR] loading target index DB: {e}")

def save_forwarded_id(unique_id):
    try:
        forwarded_unique_ids.add(unique_id)
        with open(DUPLICATE_DB_FILE, "a") as f:
            f.write(f"{unique_id}\n")
    except Exception as e:
        print(f"[DB ERR] saving duplicate ID: {e}")

def load_index_db():
    global movie_index
    if os.path.exists(INDEX_DB_FILE):
        try:
            with open(INDEX_DB_FILE, "r") as f:
                movie_index = json.load(f)
        except Exception as e:
            print(f"[DB ERR] loading index DB: {e}")
            movie_index = get_default_index()
    else:
        movie_index = get_default_index()

def save_index_db():
    try:
        with open(INDEX_DB_FILE, "w") as f:
            json.dump(movie_index, f, indent=2)
    except Exception as e:
        print(f"[DB ERR] saving index DB: {e}")
# -------------------------


def only_admin(_, __, m):
    # Ab message User account ko aa raha hai
    return m.from_user and m.from_user.id == ADMIN_ID

def _is_invite_link(s: str) -> bool:
    return bool(re.search(r"(t\.me\/\+|joinchat\/|\?startinvite=|\?invite=)", s))

async def resolve_chat_id(client: Client, ref: str | int):
    if isinstance(ref, int) or (isinstance(ref, str) and ref.lstrip("-").isdigit()):
        try:
            chat = await client.get_chat(int(ref))
            return chat
        except Exception:
            pass
    if isinstance(ref, str) and _is_invite_link(ref):
        try:
            chat = await client.join_chat(ref)
            return chat
        except UserAlreadyParticipant:
            chat = await client.get_chat(ref)
            return chat
        except (InviteHashExpired, InviteHashInvalid) as e:
            raise RuntimeError(f"❌ Invite link invalid/expired: {e}")
        except ChatAdminRequired as e:
            raise RuntimeError(f"❌ Need admin to use this invite: {e}")
    try:
        chat = await client.get_chat(ref)
        return chat
    except PeerIdInvalid:
        raise RuntimeError("❌ Peer not known. **Run /sync first!**")
    except RPCError as e:
        raise RuntimeError(f"❌ Resolve failed: {e}")

# --- /start command (Updated) ---
START_MESSAGE = """
**🚀 Welcome, Admin! (Stable Indexer Bot)**

Yeh bot ab do stages me kaam karta hai:
1.  **Index**: Source/Target channels ko scan karke local JSON file banata hai.
2.  **Forward**: Uss JSON file se movies ko target channel par bhejta hai.

**Workflow:**
1.  `/sync` - (Important) Pehli baar zaroor chalayein.
2.  `/index_target <target_chat_id>` - Target ko scan karo (sirf 1 baar).
3.  `/index <source_channel_id>` - Source ko scan karo.
4.  `/set_target <target_channel_id>` - Target ko set karo.
5.  `/start_forward`

**Available Commands:**
* `/index <chat_id>` - Source channel ko scan karke `movie_database.json` banata hai.
* `/index_target <chat_id>` - Target channel ko scan karke `target_index.json` banata hai (Duplicates ke liye).
* `/clear_index` - Source index (`.json`) ko delete karta hai.
* `/clear_target_index` - Target index (`.json`) ko delete karta hai.
* `/set_target <chat_id>` - Target channel set karein.
* `/start_forward` - JSON database se forwarding shuru karta hai.
* `/set_limit <number>` - (Optional) Max kitni movies forward karni hain.
* `/mode <copy/forward>` - `copy` (default) ya `forward`.
* `/status` - Current settings aur databases ka status dikhata hai.
* `/sync` - Bot ke local cache ko Telegram ke saath sync karta hai.
* `/ping` - Bot zinda hai ya nahi.
* `/start` - Yeh help message dikhata hai.
"""

@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_cmd(_, message):
    await message.reply(START_MESSAGE)
# ---------------------------

# --- /index (Source) ---
@app.on_message(filters.command("index") & filters.create(only_admin))
async def index_channel_cmd(_, message):
    global movie_index
    try:
        source_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index -100123...` or `/index @channel`")
        return

    async def runner():
        global movie_index
        try:
            chat = await resolve_chat_id(app, source_ref)
            src_id = chat.id
            src_name = chat.title or chat.username
        except Exception as e:
            await message.reply(str(e))
            return
        
        movie_index = get_default_index()
        movie_index["source_channel_id"] = src_id
        movie_index["source_channel_name"] = src_name
        
        status = await message.reply(f"⏳ Source Indexing shuru ho raha hai: `{src_name}`...\n(Stage 1: Videos)")

        processed_stage1 = 0
        processed_stage2 = 0
        found_count = 0
        
        try:
            # Stage 1: Videos
            async for m in app.search_messages(src_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                processed_stage1 += 1
                try:
                    if not m.video or not m.video.file_unique_id: continue
                    unique_id = m.video.file_unique_id
                    if unique_id not in movie_index["movies"]:
                        movie_index["movies"][unique_id] = { "message_id": m.id }
                        found_count += 1
                except Exception as e: print(f"[INDEX S1 ERR] Msg {m.id}: {e}")
                
                if processed_stage1 % 500 == 0:
                    try: await status.edit(f"⏳ Indexing Source... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")
                    except FloodWait: pass 

            await status.edit(f"⏳ Indexing Source... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")

            # Stage 2: Documents (Files)
            async for m in app.search_messages(src_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                    if not m.document.file_unique_id: continue
                    unique_id = m.document.file_unique_id
                    if unique_id not in movie_index["movies"]:
                        movie_index["movies"][unique_id] = { "message_id": m.id }
                        found_count += 1
                except Exception as e: print(f"[INDEX S2 ERR] Msg {m.id}: {e}")

                if processed_stage2 % 500 == 0:
                    try: await status.edit(f"⏳ Indexing Source... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {found_count} unique")
                    except FloodWait: pass

            save_index_db()
            await status.edit(f"🎉 Source Indexing Complete!\nChannel: `{src_name}`\nFound: **{found_count}** unique movies.\n\nDatabase ko `movie_database.json` me save kar diya hai.")

        except Exception as e:
            await status.edit(f"❌ Source Indexing Error: `{e}`")

    # Bot ke loop me task run karo
    app.loop.create_task(runner())
# ---------------------------------

# --- /index_target ---
@app.on_message(filters.command("index_target") & filters.create(only_admin))
async def index_target_cmd(_, message):
    try:
        target_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_target -100123...` or `/index_target @channel`")
        return

    async def runner():
        try:
            chat = await resolve_chat_id(app, target_ref)
            tgt_id = chat.id
            tgt_name = chat.title or chat.username
        except Exception as e:
            await message.reply(str(e))
            return
        
        target_movie_ids = set()
        status = await message.reply(f"⏳ Target Indexing shuru ho raha hai: `{tgt_name}`...\n(Stage 1: Videos)")

        processed_stage1 = 0
        processed_stage2 = 0
        found_count = 0
        
        try:
            # Stage 1: Videos
            async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                processed_stage1 += 1
                try:
                    if not m.video or not m.video.file_unique_id: continue
                    unique_id = m.video.file_unique_id
                    if unique_id not in target_movie_ids:
                        target_movie_ids.add(unique_id)
                        found_count += 1
                except Exception as e: print(f"[INDEX_TGT S1 ERR] Msg {m.id}: {e}")
                
                if processed_stage1 % 500 == 0:
                    try: await status.edit(f"⏳ Indexing Target... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")
                    except FloodWait: pass 

            await status.edit(f"⏳ Indexing Target... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")

            # Stage 2: Documents (Files)
            async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                    if not m.document.file_unique_id: continue
                    unique_id = m.document.file_unique_id
                    if unique_id not in target_movie_ids:
                        target_movie_ids.add(unique_id)
                        found_count += 1
                except Exception as e: print(f"[INDEX_TGT S2 ERR] Msg {m.id}: {e}")

                if processed_stage2 % 500 == 0:
                    try: await status.edit(f"⏳ Indexing Target... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {found_count} unique")
                    except FloodWait: pass
            
            with open(TARGET_INDEX_DB_FILE, "w") as f:
                json.dump(list(target_movie_ids), f)
            
            # DBs ko reload karo
            load_forwarded_ids()
            
            await status.edit(f"🎉 Target Indexing Complete!\nChannel: `{tgt_name}`\nFound: **{found_count}** existing movies.\n\nDuplicate list (`{TARGET_INDEX_DB_FILE}`) update ho gayi hai.")

        except Exception as e:
            await status.edit(f"❌ Target Indexing Error: `{e}`")

    app.loop.create_task(runner())
# ------------------------------------

# --- Clear Commands ---
@app.on_message(filters.command("clear_index") & filters.create(only_admin))
async def clear_index_cmd(_, message):
    global movie_index
    try:
        if os.path.exists(INDEX_DB_FILE):
            os.remove(INDEX_DB_FILE)
            movie_index = get_default_index()
            await message.reply(f"✅ Source index (`{INDEX_DB_FILE}`) delete kar diya hai.")
        else:
            await message.reply(f"ℹ️ Source index pehle se hi khaali hai.")
    except Exception as e:
        await message.reply(f"❌ Source index delete nahi kar paaya: {e}")

@app.on_message(filters.command("clear_target_index") & filters.create(only_admin))
async def clear_target_index_cmd(_, message):
    try:
        if os.path.exists(TARGET_INDEX_DB_FILE):
            os.remove(TARGET_INDEX_DB_FILE)
            load_forwarded_ids() # Reload karo
            await message.reply(f"✅ Target index (`{TARGET_INDEX_DB_FILE}`) delete kar diya hai. Duplicate set reload ho gaya.")
        else:
            await message.reply(f"ℹ️ Target index pehle se hi khaali hai.")
    except Exception as e:
        await message.reply(f"❌ Target index delete nahi kar paaya: {e}")
# ------------------------------------

@app.on_message(filters.command("set_target") & filters.create(only_admin))
async def set_target(_, message):
    global target_channel
    try:
        target_channel = message.text.split(" ", 1)[1].strip()
        await message.reply(f"✅ Target set: `{target_channel}`")
    except:
        await message.reply("❌ Usage:\n`/set_target -100123...` or `/set_target @channel`")

@app.on_message(filters.command("set_limit") & filters.create(only_admin))
async def set_limit(_, message):
    global limit_messages
    try:
        limit_messages = int(message.text.split(" ", 1)[1].strip())
        await message.reply(f"✅ Limit set: `{limit_messages}`")
    except:
        await message.reply("❌ Usage: `/set_limit 20000`")

@app.on_message(filters.command("mode") & filters.create(only_admin))
async def set_mode(_, message):
    global mode_copy
    try:
        arg = message.text.split(" ", 1)[1].strip().lower()
        if arg in ("copy", "c"):
            mode_copy = True
            await message.reply("✅ Mode set to **COPY** (no forwarded tag).")
        elif arg in ("forward", "f"):
            mode_copy = False
            await message.reply("✅ Mode set to **FORWARD** (shows forwarded from).")
        else:
            await message.reply("❌ Usage: `/mode copy` or `/mode forward`")
    except:
        await message.reply("❌ Usage: `/mode copy` or `/mode forward`")

# --- Stop Button ---
STOP_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Forwarding", callback_data="stop_fwd")]])
@app.on_callback_query(filters.regex("^stop_fwd$"))
async def cb_stop_forward(client, query):
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ Not allowed!", show_alert=True)
        return
        
    global is_forwarding
    is_forwarding = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try: await query.message.edit_text("🛑 Stop requested. Finishing current batch...", reply_markup=None)
    except: pass
# -------------------

@app.on_message(filters.command("start_forward") & filters.create(only_admin))
async def start_forward(_, message):
    global forwarded_count, is_forwarding

    async def runner():
        global forwarded_count, is_forwarding

        if not target_channel:
            await message.reply("⚠ Pehle `/set_target` set karo.")
            return
            
        if not movie_index["source_channel_id"] or not movie_index["movies"]:
            await message.reply("⚠ Movie index khaali hai. Pehle `/index <channel_id>` chalao.")
            return

        try:
            # Target ko resolve karo
            tgt_chat = await resolve_chat_id(app, target_channel)
            tgt = tgt_chat.id
            src = movie_index["source_channel_id"]
            src_name = movie_index["source_channel_name"]
        except Exception as e:
            await message.reply(str(e))
            return

        is_forwarding = True
        forwarded_count = 0
        duplicate_count = 0
        processed_count = 0
        total_to_forward = len(movie_index["movies"])

        status = await message.reply(
            f"⏳ Forwarding shuru ho raha hai...\n"
            f"Source (Cache): `{src_name}`\n"
            f"Target: `{tgt_chat.title or tgt_chat.username}`\n"
            f"Total Movies in Index: `{total_to_forward}`\n"
            f"Total Duplicates (Loaded): `{len(forwarded_unique_ids)}`",
            reply_markup=STOP_BUTTON
        )
        
        movies_list = list(movie_index["movies"].items())
        
        try:
            for unique_id, data in movies_list:
                processed_count += 1
                message_id = data["message_id"]

                try:
                    if not is_forwarding: break
                    
                    if unique_id in forwarded_unique_ids:
                        duplicate_count += 1
                        continue
                    
                    if mode_copy:
                        await app.copy_message(tgt, src, message_id)
                    else:
                        await app.forward_messages(tgt, src, message_id)
                    
                    save_forwarded_id(unique_id) 
                    forwarded_count += 1
                    
                    # Stability ke liye pause
                    await asyncio.sleep(PER_MSG_DELAY) 
                    
                except FloodWait as e:
                    await status.edit_text(f"⏳ FloodWait: sleeping {e.value}s…", reply_markup=STOP_BUTTON)
                    await asyncio.sleep(e.value)
                except (MessageIdInvalid, MessageAuthorRequired):
                    print(f"[FORWARD ERR] Skipping deleted/invalid msg {message_id}")
                    continue
                except RPCError as e:
                    print(f"[FORWARD RPCError] Skipping msg {message_id}: {e}")
                    continue
                except Exception as e:
                    print(f"[FORWARD ERROR] Skipping msg {message_id}: {e}")
                    continue
                
                # Status update har 50 movies (ya 500 processed) par
                if (forwarded_count % 50 == 0) or (processed_count % 500 == 0):
                    try:
                        await status.edit_text(
                            f"✅ Fwd: `{forwarded_count}` / {(limit_messages or '∞')}, 🔍 Dup: `{duplicate_count}`\n"
                            f"⏳ Processed: {processed_count} / {total_to_forward}",
                            reply_markup=STOP_BUTTON
                        )
                    except FloodWait: pass 

                if limit_messages and forwarded_count >= limit_messages:
                    is_forwarding = False
                    break

        except Exception as e:
            await status.edit_text(f"❌ Error: `{e}`", reply_markup=None)
            is_forwarding = False
            return

        await status.edit_text(
            f"🎉 Completed\n"
            f"✅ Total Movies Forwarded: `{forwarded_count}`\n"
            f"🔍 Duplicates Skipped: `{duplicate_count}`",
            reply_markup=None
        )

    app.loop.create_task(runner())

@app.on_message(filters.command("stop_forward") & filters.create(only_admin))
async def stop_forward(_, message):
    global is_forwarding
    is_forwarding = False
    await message.reply("🛑 Stop requested.")

@app.on_message(filters.command("status") & filters.create(only_admin))
async def status_cmd(_, message):
    total_in_fwd_db = 0
    if os.path.exists(DUPLICATE_DB_FILE):
        with open(DUPLICATE_DB_FILE, "r") as f:
            total_in_fwd_db = len(f.readlines())
            
    total_in_target_db = 0
    if os.path.exists(TARGET_INDEX_DB_FILE):
        try:
            with open(TARGET_INDEX_DB_FILE, "r") as f:
                total_in_target_db = len(json.load(f))
        except: pass # File khaali ho sakti hai

    total_in_index = len(movie_index.get('movies', {}))
    
    await message.reply(
        f"📊 Status\n"
        f"Target: `{target_channel}`\n"
        f"Mode: `{'COPY' if mode_copy else 'FORWARD'}`\n"
        f"Limit: `{limit_messages}`\n"
        f"--- Session ---\n"
        f"Forwarded: `{forwarded_count}`\n"
        f"--- Databases ---\n"
        f"Indexed Source Movies: `{total_in_index}` (from `{movie_index.get('source_channel_name', 'N/A')}`)\n"
        f"Indexed Target Movies: `{total_in_target_db}` (in `{TARGET_INDEX_DB_FILE}`)\n"
        f"Bot-Forwarded Movies: `{total_in_fwd_db}` (in `{DUPLICATE_DB_FILE}`)\n"
        f"---"
        f"Total Duplicates in Memory: `{len(forwarded_unique_ids)}`"
    )

@app.on_message(filters.command("sync") & filters.create(only_admin))
async def sync_chats(_, message):
    async def runner():
        status = await message.reply("⏳ Syncing User Account chats...")
        count = 0
        try:
            async for _ in app.get_dialogs():
                count += 1
            await status.edit(f"✅ User Account Cache synced! Found {count} chats.")
        except Exception as e:
            await status.edit(f"❌ Sync failed: {e}")
    app.loop.create_task(runner())


@app.on_message(filters.command("ping") & filters.create(only_admin))
async def ping(_, message):
    await message.reply("✅ Alive | Polling | Ready")

# Start loop
print("Loading databases...")
load_forwarded_ids()
load_index_db()
print(f"Loaded {len(forwarded_unique_ids)} total unique IDs (Bot Fwd + Target Index) into memory.")
print(f"Loaded {len(movie_index.get('movies', {}))} indexed movies from {INDEX_DB_FILE}")
print("✅ UserBot ready — send commands.")
app.run()
