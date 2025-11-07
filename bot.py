from keep_alive import keep_alive
keep_alive()

import os, time, re
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, RPCError
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Session via string (Pyrogram v2)
app = Client("user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

# --- Runtime state ---
source_channel = None
target_channel = None
limit_messages = None
forwarded_count = 0     # Is session me kitne forward hue
is_forwarding = False
mode_copy = True
BATCH_SIZE = 100        # <-- FIX: Wapas 100 kar diya. Yeh UserBot ke liye best hai.
# 300 karne se 21*3 = 63 sec ka wait hota hai. 100 se sirf 21 sec ka wait hoga.

# --- Duplicate Check ---
DUPLICATE_DB_FILE = "forwarded_unique_ids.txt"
forwarded_unique_ids = set()

def load_forwarded_ids():
    """Bot start hone par purane IDs ko load karta hai"""
    global forwarded_unique_ids
    if os.path.exists(DUPLICATE_DB_FILE):
        try:
            with open(DUPLICATE_DB_FILE, "r") as f:
                for line in f:
                    forwarded_unique_ids.add(line.strip())
        except Exception as e:
            print(f"Error loading duplicate DB: {e}")

def save_forwarded_id(unique_id):
    """Naye forward hue ID ko file me save karta hai"""
    try:
        forwarded_unique_ids.add(unique_id)
        with open(DUPLICATE_DB_FILE, "a") as f:
            f.write(f"{unique_id}\n")
    except Exception as e:
        print(f"Error saving duplicate ID: {e}")
# -------------------------


def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def _is_invite_link(s: str) -> bool:
    return bool(re.search(r"(t\.me\/\+|joinchat\/|\?startinvite=|\?invite=)", s))

async def resolve_chat_id(client: Client, ref: str | int):
    """Chat ID ko resolve karta hai"""
    if isinstance(ref, int) or (isinstance(ref, str) and ref.lstrip("-").isdigit()):
        try:
            chat = await client.get_chat(int(ref))
            return chat.id
        except Exception:
            pass

    if isinstance(ref, str) and _is_invite_link(ref):
        try:
            chat = await client.join_chat(ref)
            return chat.id
        except UserAlreadyParticipant:
            chat = await client.get_chat(ref)
            return chat.id
        except (InviteHashExpired, InviteHashInvalid) as e:
            raise RuntimeError(f"❌ Invite link invalid/expired: {e}")
        except ChatAdminRequired as e:
            raise RuntimeError(f"❌ Need admin to use this invite: {e}")

    try:
        chat = await client.get_chat(ref)
        return chat.id
    except PeerIdInvalid:
        raise RuntimeError("❌ Peer not known. Make sure this account joined that chat. **Run /sync first!**")
    except RPCError as e:
        raise RuntimeError(f"❌ Resolve failed: {e}")

# --- Naya /start command ---
START_MESSAGE = """
**🚀 Welcome, Admin!**

Yeh aapka personal Movie Forwarder Bot hai.
Aap isse movies ko ek channel se doosre me (duplicates skip karke) copy kar sakte hain.

**Available Commands:**

1.  **Setup Commands:**
    * `/sync` - Bot ke local cache ko Telegram ke saath sync karta hai. **(Important: Hamesha pehle yeh chalayein)**
    * `/set_source <chat_id>` - Source channel set karein (ID, @username, ya invite link).
    * `/set_target <chat_id>` - Target channel set karein (ID ya @username).
    * `/set_limit <number>` - (Optional) Max kitni movies forward karni hain.
    * `/mode <copy/forward>` - `copy` (default, "Forwarded from" nahi dikhega) ya `forward`.

2.  **Execution Commands:**
    * `/meet` - Source aur Target ko check karta hai ki bot unhe access kar sakta hai ya nahi.
    * `/start_forward` - Movie forwarding process shuru karta hai.
    * `/stop_forward` - (Command) Process ko rokta hai (Stop button behtar hai).

3.  **Status Commands:**
    * `/status` - Current settings aur database me kitni movies hain, yeh dikhata hai.
    * `/ping` - Check karta hai ki bot zinda hai ya nahi.
    * `/start` - Yeh help message dikhata hai.
"""

@app.on_message(filters.command("start") & filters.create(only_admin))
def start_cmd(_, message):
    message.reply(START_MESSAGE)
# ---------------------------

@app.on_message(filters.command("set_source") & filters.create(only_admin))
def set_source(_, message):
    global source_channel
    try:
        source_channel = message.text.split(" ", 1)[1].strip()
        message.reply(f"✅ Source set: `{source_channel}`\nTip: run `/meet` once.")
    except:
        message.reply("❌ Usage:\n`/set_source -100123...` or `/set_source @channel` or invite link")

@app.on_message(filters.command("set_target") & filters.create(only_admin))
def set_target(_, message):
    global target_channel
    try:
        target_channel = message.text.split(" ", 1)[1].strip()
        message.reply(f"✅ Target set: `{target_channel}`\nTip: run `/meet` once.")
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

@app.on_message(filters.command("meet") & filters.create(only_admin))
def meet_cmd(_, message):
    async def runner():
        if not source_channel or not target_channel:
            await message.reply("⚠ Pehle `/set_source` & `/set_target` set karo.")
            return
        try:
            src_id = await resolve_chat_id(app, source_channel)
            tgt_id = await resolve_chat_id(app, target_channel)
            await message.reply(f"🤝 Met peers:\nSource: `{src_id}`\nTarget: `{tgt_id}`\nNow run `/start_forward`.")
        except Exception as e:
            await message.reply(f"{str(e)}\n\n**Tip:** Agar 'Peer not known' error aata hai, toh pehle `/sync` command chalao.")
    app.loop.create_task(runner())


# --- Naya Stop Button ---
STOP_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Forwarding", callback_data="stop_fwd")]])

@app.on_callback_query(filters.regex("^stop_fwd$") & filters.create(only_admin))
async def cb_stop_forward(_, query):
    """Stop button ko handle karta hai"""
    global is_forwarding
    is_forwarding = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try:
        await query.message.edit_text("🛑 Stop requested. Finishing current batch...", reply_markup=None)
    except:
        pass # Message nahi badla toh error aayega, ignore karo
# -------------------------


@app.on_message(filters.command("start_forward") & filters.create(only_admin))
def start_forward(_, message):
    global forwarded_count, is_forwarding

    async def runner():
        global forwarded_count, is_forwarding

        if not source_channel or not target_channel:
            await message.reply("⚠ Pehle `/set_source` & `/set_target` set karo.")
            return

        try:
            src = await resolve_chat_id(app, source_channel)
            tgt = await resolve_chat_id(app, target_channel)
        except Exception as e:
            await message.reply(str(e))
            return

        is_forwarding = True
        forwarded_count = 0 # Session counter reset
        skipped_count = 0
        duplicate_count = 0
        
        status = await message.reply("⏳ Starting movie forwarding...", reply_markup=STOP_BUTTON)

        offset_id = 0
        while True:
            if not is_forwarding:
                await status.edit_text(f"🛑 Stopped\n✅ Movies Forwarded: `{forwarded_count}`\n🔍 Duplicates: `{duplicate_count}`", reply_markup=None)
                return

            try:
                # 1. 100 message ka batch maangega.
                # Isme internal 21-sec wait ho sakta hai (jaisa aapne logs me dekha).
                batch = []
                async for m in app.get_chat_history(src, offset_id=offset_id, limit=BATCH_SIZE):
                    batch.append(m)
                if not batch:
                    break # History khatam

                # 2. Batch ko process karega (yeh fast hoga)
                for m in reversed(batch): # Order maintain karne ke liye
                    if not is_forwarding:
                        break # Batch ke beech me stop
                    
                    unique_id = None
                    if m.video:
                        unique_id = m.video.file_unique_id
                    elif m.document and m.document.mime_type and m.document.mime_type.startswith("video/"):
                        unique_id = m.document.file_unique_id
                    
                    if not unique_id:
                        skipped_count += 1
                        continue # Yeh movie nahi hai
                    
                    if unique_id in forwarded_unique_ids:
                        duplicate_count += 1
                        continue # Yeh duplicate hai
                    
                    try:
                        if mode_copy:
                            await app.copy_message(tgt, src, m.id)
                        else:
                            await app.forward_messages(tgt, src, m.id)
                        
                        save_forwarded_id(unique_id) # Success par save karo
                        forwarded_count += 1
                        
                    except FloodWait as e:
                        await status.edit_text(f"⏳ FloodWait: sleeping {e.value}s…", reply_markup=STOP_BUTTON)
                        time.sleep(e.value) # Yahan dynamically wait karega
                    except RPCError as e:
                        if "MESSAGE_COPY_FORBIDDEN" in str(e):
                            await status.edit_text("❌ Source is **Content Protected**.\nUse `/mode forward` then `/start_forward`.", reply_markup=None)
                            is_forwarding = False
                            return
                        continue # Individual message error, skip karo

                offset_id = batch[0].id
                
                # 3. <-- FIX: Status message HAR BATCH ke baad update hoga
                # Isse aapko hamesha progress dikhegi, bhale hi 0 movie mili ho.
                await status.edit_text(
                    f"✅ Movies Forwarded: `{forwarded_count}` / {(limit_messages or '∞')}\n"
                    f"🔍 Duplicates Skipped: `{duplicate_count}`\n"
                    f"🚫 Non-Movies Skipped: `{skipped_count}`\n"
                    f"⏳ Processing... (Fetching next 100)",
                    reply_markup=STOP_BUTTON
                )

                if limit_messages and forwarded_count >= limit_messages:
                    is_forwarding = False # Limit poora ho gaya
                    break

            except FloodWait as e:
                await status.edit_text(f"⏳ FloodWait: sleeping {e.value}s…", reply_markup=STOP_BUTTON)
                time.sleep(e.value)
            except PeerIdInvalid:
                await status.edit_text("❌ Peer invalid. Run `/sync` first.", reply_markup=None)
                is_forwarding = False
                return
            except Exception as e:
                # Agar 20s wala error aaye, toh woh bhi yahan dikhega
                await status.edit_text(f"❌ Error: `{e}`", reply_markup=None)
                is_forwarding = False
                return

        await status.edit_text(
            f"🎉 Completed\n"
            f"✅ Total Movies Forwarded: `{forwarded_count}`\n"
            f"🔍 Duplicates Skipped: `{duplicate_count}`\n"
            f"🚫 Non-Movies Skipped: `{skipped_count}`",
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
    total_in_db = len(forwarded_unique_ids)
    message.reply(
        f"📊 Status\n"
        f"Source: `{source_channel}`\n"
        f"Target: `{target_channel}`\n"
        f"Mode: `{'COPY' if mode_copy else 'FORWARD'}`\n"
        f"Limit: `{limit_messages}`\n"
        f"--- Session ---\n"
        f"Forwarded: `{forwarded_count}`\n"
        f"--- Database ---\n"
        f"Total Unique Movies in DB: `{total_in_db}`"
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

print("Loading duplicate database...")
load_forwarded_ids()
print(f"Loaded {len(forwarded_unique_ids)} unique file IDs from {DUPLICATE_DB_FILE}")

print("✅ UserBot ready — send commands in your control group.")
app.run()
