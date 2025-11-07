from keep_alive import keep_alive
keep_alive()

import os
import time
from pyrogram import Client, filters
from pyrogram.errors import FloodWait

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

app = Client(SESSION_STRING, api_id=API_ID, api_hash=API_HASH)

source_channel = None
target_channel = None
limit_messages = None
forwarded_count = 0
is_forwarding = False


def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID


@app.on_message(filters.command("set_source") & filters.create(only_admin))
def set_source(_, message):
    global source_channel
    try:
        source_channel = message.text.split(" ")[1]
        message.reply(f"✅ Source Set: `{source_channel}`")
    except:
        message.reply("❌ Usage:\n`/set_source -100xxxxxxxxxx`")


@app.on_message(filters.command("set_target") & filters.create(only_admin))
def set_target(_, message):
    global target_channel
    try:
        target_channel = message.text.split(" ")[1]
        message.reply(f"✅ Target Set: `{target_channel}`")
    except:
        message.reply("❌ Usage:\n`/set_target -100xxxxxxxxxx`")


@app.on_message(filters.command("set_limit") & filters.create(only_admin))
def set_limit(_, message):
    global limit_messages
    try:
        limit_messages = int(message.text.split(" ")[1])
        message.reply(f"✅ Limit Set: `{limit_messages}`")
    except:
        message.reply("❌ Usage:\n`/set_limit 15000`")


@app.on_message(filters.command("start_forward") & filters.create(only_admin))
def start_forward(_, message):
    global forwarded_count, is_forwarding

    if not source_channel or not target_channel:
        return message.reply("⚠ Pehle `/set_source` aur `/set_target` set karo")

    is_forwarding = True
    forwarded_count = 0
    status = message.reply("⏳ Starting Forward...")

    for msg in app.get_chat_history(source_channel, limit=limit_messages):
        if not is_forwarding:
            return status.edit(f"🛑 Stopped\n✅ Forwarded: `{forwarded_count}`")

        try:
            app.copy_message(
                chat_id=target_channel,
                from_chat_id=source_channel,
                message_id=msg.id
            )
            forwarded_count += 1

            if forwarded_count % 100 == 0:
                status.edit(f"✅ Forwarded `{forwarded_count}`...\n⏳ Working...")
                time.sleep(2)

        except FloodWait as e:
            status.edit(f"⏳ FloodWait: Sleeping {e.value}s")
            time.sleep(e.value)

    status.edit(f"🎉 Completed!\n✅ Total Forwarded: `{forwarded_count}`")


@app.on_message(filters.command("stop_forward") & filters.create(only_admin))
def stop_forward(_, message):
    global is_forwarding
    is_forwarding = False
    message.reply("🛑 Stop Request Sent.")


@app.on_message(filters.command("status") & filters.create(only_admin))
def status(_, message):
    message.reply(
        f"📊 Status:\n"
        f"Source: `{source_channel}`\n"
        f"Target: `{target_channel}`\n"
        f"Forwarded: `{forwarded_count}`"
    )


@app.on_message(filters.command("ping") & filters.create(only_admin))
def ping(_, message):
    message.reply("✅ Alive | Polling Mode OK")


print("✅ UserBot Started | Control from Your Group")
app.run()
