from keep_alive import keep_alive
keep_alive()

import os, time, re
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, RPCError

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Session via string (Pyrogram v2)
app = Client("user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

# Runtime state
source_channel = None   # can be -100id / @username / invite link
target_channel = None
limit_messages = None   # int
forwarded_count = 0
is_forwarding = False
mode_copy = True        # default: COPY (no "forwarded from")
BATCH_SIZE = 100

# Tiny delay to avoid hard flood (tune 0.15 ~ 0.3)
PER_MSG_DELAY = 0.2

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def _is_invite_link(s: str) -> bool:
    # FIX: Added the closing parenthesis ')' at the end of the regex pattern
    return bool(re.search(r"(t\.me\/\+|joinchat\/|\?startinvite=|\?invite=)", s))

async def resolve_chat_id(client: Client, ref: str | int):
    """
    Accepts -100ID / @username / invite link.
    Ensures peer is 'met' so get_chat_history works.
    Returns numeric chat_id (e.g., -100xxxxxxxxx).
    """
    # Numeric id
    if isinstance(ref, int) or (isinstance(ref, str) and ref.lstrip("-").isdigit()):
        try:
            chat = await client.get_chat(int(ref))
            return chat.id
        except Exception:
            pass  # fall through

    # Invite links → try join
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

    # @username or public link
    try:
        chat = await client.get_chat(ref)
        return chat.id
    except PeerIdInvalid:
        raise RuntimeError("❌ Peer not known. Make sure this account joined that chat.")
    except RPCError as e:
        raise RuntimeError(f"❌ Resolve failed: {e}")

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
    # Pre-resolve both peers; helpful for -100 ids and speed
    async def runner():
        if not source_channel or not target_channel:
            await message.reply("⚠ Pehle `/set_source` & `/set_target` set karo.")
            return
        try:
            src_id = await resolve_chat_id(app, source_channel)
            tgt_id = await resolve_chat_id(app, target_channel)
            await message.reply(f"🤝 Met peers:\nSource: `{src_id}`\nTarget: `{tgt_id}`\nNow run `/start_forward`.")
        except Exception as e:
            await message.reply(str(e))
    app.loop.create_task(runner())

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
        forwarded_count = 0
        status = await message.reply("⏳ Starting forwarding...")

        # Pagination using offset_id
        fetched = 0
        offset_id = 0
        while True:
            if not is_forwarding:
                await status.edit(f"🛑 Stopped\n✅ Forwarded: `{forwarded_count}`")
                return

            try:
                # get_chat_history newest→oldest; use offset_id to paginate
                batch = []
                async for m in app.get_chat_history(src, offset_id=offset_id, limit=BATCH_SIZE):
                    batch.append(m)
                if not batch:
                    break

                # Process oldest first within the batch to keep order
                for m in reversed(batch):
                    try:
                        if mode_copy:
                            await app.copy_message(tgt, src, m.id)
                        else:
                            await app.forward_messages(tgt, src, m.id)
                        forwarded_count += 1
                        time.sleep(PER_MSG_DELAY)
                    except FloodWait as e:
                        await status.edit(f"⏳ FloodWait: sleeping {e.value}s…")
                        time.sleep(e.value)
                    except RPCError as e:
                        # Content-protected / other errors
                        if "MESSAGE_COPY_FORBIDDEN" in str(e):
                            await status.edit("❌ Source is **Content Protected**.\nUse `/mode forward` then `/start_forward`.")
                            return
                        # Skip individual bad message & continue
                        continue

                # next page
                offset_id = batch[0].id  # oldest id of this page
                fetched += len(batch)

                if forwarded_count % 100 == 0:
                    await status.edit(f"✅ Forwarded: `{forwarded_count}` / {(limit_messages or '∞')}\n⏳ Working…")

                # Respect overall limit if set
                if limit_messages and forwarded_count >= limit_messages:
                    break

            except FloodWait as e:
                await status.edit(f"⏳ FloodWait: sleeping {e.value}s…")
                time.sleep(e.value)
            except PeerIdInvalid:
                await status.edit("❌ Peer invalid again. Run `/meet` first & ensure this account joined both chats.")
                return
            except Exception as e:
                await status.edit(f"❌ Error: `{e}`")
                return

        await status.edit(f"🎉 Completed\n✅ Total Forwarded: `{forwarded_count}`")

    app.loop.create_task(runner())

@app.on_message(filters.command("stop_forward") & filters.create(only_admin))
def stop_forward(_, message):
    global is_forwarding
    is_forwarding = False
    message.reply("🛑 Stop requested.")

@app.on_message(filters.command("status") & filters.create(only_admin))
def status_cmd(_, message):
    message.reply(
        f"📊 Status\n"
        f"Source: `{source_channel}`\n"
        f"Target: `{target_channel}`\n"
        f"Mode: `{'COPY' if mode_copy else 'FORWARD'}`\n"
        f"Forwarded: `{forwarded_count}`\n"
        f"Limit: `{limit_messages}`"
    )

@app.on_message(filters.command("ping") & filters.create(only_admin))
def ping(_, message):
    message.reply("✅ Alive | Polling | Ready")

print("✅ UserBot ready — send commands in your control group.")
app.run()
