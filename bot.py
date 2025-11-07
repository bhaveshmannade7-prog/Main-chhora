from keep_alive import keep_alive
keep_alive()

import os, time, re, json
import asyncio
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, RPCError
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH") # <-- FIX: Extra ')' hata diya
SESSION_STRING = os.getenv("SESSION_STRING") # <-- FIX: Extra ')' hata diya
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Session via string (Pyrogram v2)
app = Client("user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

# --- Runtime state ---
# source_channel ab JSON se aayega
target_channel = None
limit_messages = None
forwarded_count = 0
is_forwarding = False
mode_copy = True

# --- Database Files ---
DUPLICATE_DB_FILE = "forwarded_unique_ids.txt" # Jo bhej diya hai
INDEX_DB_FILE = "movie_database.json"          # Jo source me mila hai

forwarded_unique_ids = set()
movie_index = {
    "source_channel_id": None,
    "source_channel_name": None,
    "movies": {} # 'unique_id': { 'message_id': 123 }
}

def load_forwarded_ids():
    global forwarded_unique_ids
    if os.path.exists(DUPLICATE_DB_FILE):
        try:
            with open(DUPLICATE_DB_FILE, "r") as f:
                for line in f:
                    forwarded_unique_ids.add(line.strip())
        except Exception as e:
            print(f"Error loading duplicate DB: {e}")

def save_forwarded_id(unique_id):
    try:
        forwarded_unique_ids.add(unique_id)
        with open(DUPLICATE_DB_FILE, "a") as f:
            f.write(f"{unique_id}\n")
    except Exception as e:
        print(f"Error saving duplicate ID: {e}")

def load_index_db():
    global movie_index
    if os.path.exists(INDEX_DB_FILE):
        try:
            with open(INDEX_DB_FILE, "r") as f:
                movie_index = json.load(f)
        except Exception as e:
            print(f"Error loading index DB: {e}")

def save_index_db():
    try:
        with open(INDEX_DB_FILE, "w") as f:
            json.dump(movie_index, f, indent=2)
    except Exception as e:
        print(f"Error saving index DB: {e}")
# -------------------------


def only_admin(_, __, m):
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
        raise RuntimeError("❌ Peer not known. Make sure this account joined that chat. **Run /sync first!**")
    except RPCError as e:
        raise RuntimeError(f"❌ Resolve failed: {e}")

# --- /start command (Updated) ---
START_MESSAGE = """
**🚀 Welcome, Admin! (JSON Indexer Bot)**

Yeh bot ab do stages me kaam karta hai:
1.  **Index**: Source channel ko scan karke ek local JSON file banata hai.
2.  **Forward**: Uss JSON file se movies ko target channel par bhejta hai.

**Naya Workflow:**
1.  `/sync` - (Agar zaroori ho)
2.  `/index <source_channel_id>` - **(Naya Kadam)** Movies ko scan aur save karein.
3.  `/set_target <target_channel_id>`
4.  `/start_forward`

**Available Commands:**

* `/index <chat_id>` - Source channel ko scan karke `movie_database.json` banata hai.
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
def start_cmd(_, message):
    message.reply(START_MESSAGE)
# ---------------------------

# --- SET_SOURCE ab INDEX hai ---
@app.on_message(filters.command("index") & filters.create(only_admin))
def index_channel_cmd(_, message):
    global movie_index
    try:
        source_ref = message.text.split(" ", 1)[1].strip()
    except:
        message.reply("❌ Usage:\n`/index -100123...` or `/index @channel` or invite link")
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
        
        # Naya index shuru karo
        movie_index = {
            "source_channel_id": src_id,
            "source_channel_name": src_name,
            "movies": {}
        }
        
        status = await message.reply(f"⏳ Indexing shuru ho raha hai: `{src_name}`...\n(Stage 1: Videos)")

        processed_stage1 = 0
        processed_stage2 = 0
        found_count = 0
        
        try:
            # Stage 1: Videos
            async for m in app.search_messages(src_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                processed_stage1 += 1
                try:
                    if not m.video or not m.video.file_unique_id:
                        continue
                    
                    unique_id = m.video.file_unique_id
                    if unique_id not in movie_index["movies"]:
                        movie_index["movies"][unique_id] = { "message_id": m.id }
                        found_count += 1
                except Exception as e:
                    print(f"[INDEX S1 ERR] Msg {m.id}: {e}")
                    continue
                
                if processed_stage1 % 500 == 0:
                    try:
                        await status.edit(f"⏳ Indexing... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")
                    except FloodWait:
                        pass # Status update ko skip karo agar floodwait hai

            await status.edit(f"⏳ Indexing... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")

            # Stage 2: Documents (Files)
            async for m in app.search_messages(src_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")):
                        continue
                    if not m.document.file_unique_id:
                        continue
                    
                    unique_id = m.document.file_unique_id
                    if unique_id not in movie_index["movies"]:
                        movie_index["movies"][unique_id] = { "message_id": m.id }
                        found_count += 1
                except Exception as e:
                    print(f"[INDEX S2 ERR] Msg {m.id}: {e}")
                    continue

                if processed_stage2 % 500 == 0:
                    try:
                        await status.edit(f"⏳ Indexing... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {found_count} unique")
                    except FloodWait:
                        pass

            save_index_db()
            await status.edit(f"🎉 Indexing Complete!\nChannel: `{src_name}`\nFound: **{found_count}** unique movies.\n\nDatabase ko `movie_database.json` me save kar diya hai.\nAb `/set_target` aur `/start_forward` chalaein.")

        except Exception as e:
            await status.edit(f"❌ Indexing Error: `{e}`")

    app.loop.create_task(runner())
# ---------------------------------

@app.on_message(filters.command("set_target") & filters.create(only_admin))
def set_target(_, message):
    global target_channel
    try:
        target_channel = message.text.split(" ", 1)[1].strip()
        message.reply(f"✅ Target set: `{target_channel}`")
    except:
        message.reply("❌ Usage:\n`/set_target -100123...` or `/set_target @channel`")

@app.on_message(filters.command("set_limit") & filters.create(only_admin))
def set_limit(_, message):
    global limit_messages
    try:
        limit_messages = int(message.text.split(" ", 1)[1].strip())
        message.reply(f"✅ Limit set: `{limit_messages}`")
    except:
        message.reply("❌ Usage: `/set_limit 20000`")

@app.on_message(filters.command("mode") & filters.create(only_admin))
def set_mode(_, message):
    global mode_copy
    try:
        arg = message.text.split(" ", 1)[1].strip().lower()
        if arg in ("copy", "c"):
            mode_copy = True
            message.reply("✅ Mode set to **COPY** (no forwarded tag).")
        elif arg in ("forward", "f"):
            mode_copy = False
            message.reply("✅ Mode set to **FORWARD** (shows forwarded from).")
        else:
            message.reply("❌ Usage: `/mode copy` or `/mode forward`")
    except:
        message.reply("❌ Usage: `/mode copy` or `/mode forward`")

@app.on_message(filters.command("meet"))
def meet_cmd_removed(_, message):
    message.reply("ℹ️ `/meet` command ki ab zaroorat nahi hai.\n`/index` command ab source channel ko check kar leta hai.")

# --- Stop Button ---
STOP_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Forwarding", callback_data="stop_fwd")]])
@app.on_callback_query(filters.regex("^stop_fwd$") & filters.create(only_admin))
async def cb_stop_forward(_, query):
    global is_forwarding
    is_forwarding = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try:
        await query.message.edit_text("🛑 Stop requested. Finishing current batch...", reply_markup=None)
    except:
        pass
# -------------------

@app.on_message(filters.command("start_forward") & filters.create(only_admin))
def start_forward(_, message):
    global forwarded_count, is_forwarding

    async def runner():
        global forwarded_count, is_forwarding

        # Check karo ki target set hai ya nahi
        if not target_channel:
            await message.reply("⚠ Pehle `/set_target` set karo.")
            return
            
        # Check karo ki index file hai ya nahi
        if not movie_index["source_channel_id"] or not movie_index["movies"]:
            await message.reply("⚠ Movie index khaali hai. Pehle `/index <channel_id>` chalao.")
            return

        try:
            # Target ko resolve karo
            tgt_chat = await resolve_chat_id(app, target_channel)
            tgt = tgt_chat.id
            # Source ko JSON se lo
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
            f"Total Movies in Index: `{total_to_forward}`",
            reply_markup=STOP_BUTTON
        )
        
        # Ab local JSON se loop karo (bohot fast)
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
                    
                except FloodWait as e:
                    await status.edit_text(f"⏳ FloodWait: sleeping {e.value}s…", reply_markup=STOP_BUTTON)
                    await asyncio.sleep(e.value)
                except (MessageIdInvalid, MessageAuthorRequired):
                    # Original message delete ho gayi, skip karo
                    print(f"[FORWARD ERR] Skipping deleted/invalid msg {message_id}")
                    continue
                except RPCError as e:
                    print(f"[FORWARD RPCError] Skipping msg {message_id}: {e}")
                    continue
                except Exception as e:
                    print(f"[FORWARD ERROR] Skipping msg {message_id}: {e}")
                    continue
                
                if (forwarded_count % 25 == 0) or (processed_count % 100 == 0):
                    try:
                        await status.edit_text(
                            f"✅ Fwd: `{forwarded_count}` / {(limit_messages or '∞')}, 🔍 Dup: `{duplicate_count}`\n"
                            f"⏳ Processed: {processed_count} / {total_to_forward}",
                            reply_markup=STOP_BUTTON
                        )
                    except FloodWait:
                        pass

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
def stop_forward(_, message):
    global is_forwarding
    is_forwarding = False
    message.reply("🛑 Stop requested.")

@app.on_message(filters.command("status") & filters.create(only_admin))
def status_cmd(_, message):
    total_in_fwd_db = len(forwarded_unique_ids)
    total_in_index = len(movie_index["movies"])
    
    message.reply(
        f"📊 Status\n"
        f"Target: `{target_channel}`\n"
        f"Mode: `{'COPY' if mode_copy else 'FORWARD'}`\n"
        f"Limit: `{limit_messages}`\n"
        f"--- Session ---\n"
        f"Forwarded: `{forwarded_count}`\n"
        f"--- Databases ---\n"
        f"Indexed Movies: `{total_in_index}` (from `{movie_index['source_channel_name']}`)\n"
        f"Forwarded Movies: `{total_in_fwd_db}` (in `{DUPLICATE_DB_FILE}`)"
    )

@app.on_message(filters.command("sync") & filters.create(only_admin))
def sync_chats(_, message):
    async def runner():
        status = await message.reply("⏳ Syncing chats...")
        count = 0
        try:
            async for _ in app.get_dialogs():
                count += 1
            await status.edit(f"✅ Cache synced! Found {count} chats.")
        except Exception as e:
            await status.edit(f"❌ Sync failed: {e}")
    app.loop.create_task(runner())


@app.on_message(filters.command("ping") & filters.create(only_admin))
def ping(_, message):
    message.reply("✅ Alive | Polling | Ready")

print("Loading databases...")
load_forwarded_ids()
load_index_db()
print(f"Loaded {len(forwarded_unique_ids)} forwarded IDs from {DUPLICATE_DB_FILE}")
print(f"Loaded {len(movie_index['movies'])} indexed movies from {INDEX_DB_FILE}")

print("✅ UserBot ready — send commands in your control group.")
app.run()
