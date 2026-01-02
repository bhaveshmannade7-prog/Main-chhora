from keep_alive import keep_alive
keep_alive()

import os, time, re, json
import asyncio
from pyrogram import Client, filters, enums
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, 
    PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, 
    RPCError, UsernameInvalid, ChannelPrivate 
)
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
GLOBAL_TASK_RUNNING = False 
mode_copy = True
PER_MSG_DELAY = 0.5 
BATCH_SIZE_FOR_BREAK = 250
BREAK_DURATION_SEC = 25
locked_content = None 

# --- Database Files ---
DUPLICATE_DB_FILE = "forwarded_unique_ids.txt"
# Movie Databases
MOVIE_INDEX_DB_FILE = "movie_database.json"
TARGET_MOVIE_INDEX_DB_FILE = "target_movie_index.json"
# Webseries Databases
WEBSERIES_INDEX_DB_FILE = "webseries_database.json"
TARGET_WEBSERIES_INDEX_DB_FILE = "target_webseries_index.json"
# NAYA: Full Forward Databases
FULL_SOURCE_INDEX_DB_FILE = "full_source_index.json"
FULL_TARGET_INDEX_DB_FILE = "full_target_index.json"
# Utility Databases
BAD_QUALITY_DB_FILE = "bad_quality_movies.json" 
LOCKED_CONTENT_FILE = "locked_content.txt"
EDITING_INDEX_DB_FILE = "editing_index.json"

# In-memory sets
movie_fwd_unique_ids = set()
movie_target_compound_keys = set() 
webseries_fwd_unique_ids = set()
webseries_target_compound_keys = set() 
# NAYA: Full Forward Sets
full_fwd_unique_ids = set()
full_target_compound_keys = set()

# Bad Quality Keywords
BAD_QUALITY_KEYWORDS = [
    r"cam", r"camrip", r"hdcam", r"ts", r"telesync", r"tc", 
    r"\(line\)", r"\(clean\)", r"line audio", r"bad audio",
    r"screen record", r"screener", r"hq-cam"
]
BAD_QUALITY_REGEX = re.compile(r'\b(?:' + '|'.join(BAD_QUALITY_KEYWORDS) + r')\b', re.IGNORECASE)

# --- Web Series Regex ---
EPISODE_REGEX = re.compile(
    r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(?:E|Ep|Episode)\s*(\d{1,3})(?!.*\d)",
    re.IGNORECASE | re.DOTALL
)
EPISODE_PACK_REGEX = re.compile(
    r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(?:E|Ep|Episode)\s*(\d{1,3})\s*-\s*(\d{1,3})",
    re.IGNORECASE | re.DOTALL
)
SEASON_COMPLETE_REGEX = re.compile(
    r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(Complete)",
    re.IGNORECASE | re.DOTALL
)
EPISODE_ONLY_REGEX = re.compile(
    r"(.*?)(?:Episode|Ep)\s*(\d{1,3})(?!.*\d)",
    re.IGNORECASE | re.DOTALL
)
SIMPLE_SEASON_REGEX = re.compile(
    r"\b(S\d{1,2})\b", re.IGNORECASE
)
SERIES_KEYWORDS_REGEX = re.compile(
    r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE
)
# ------------------------------------

LINK_USERNAME_REGEX = re.compile(
    r"(?:https?://[^\s]+|t\.me/[^\s]+|@[\w]+)", 
    re.IGNORECASE
)

def get_default_movie_index():
    return {
        "source_channel_id": None,
        "source_channel_name": None,
        "movies": {}
    }
movie_index = get_default_movie_index()


# --- Database Load/Save Functions ---

def load_locked_content():
    global locked_content
    if os.path.exists(LOCKED_CONTENT_FILE):
        try:
            with open(LOCKED_CONTENT_FILE, "r", encoding="utf-8") as f:
                locked_content = f.read().strip()
                if locked_content:
                    print(f"Loaded locked content: {locked_content[:50]}...")
                else:
                    print("Locked content file was empty.")
                    locked_content = None
        except Exception as e:
            print(f"[DB ERR] loading locked content file: {e}")
            locked_content = None
    else:
        print("No locked content file found.")
        locked_content = None

def save_locked_content():
    global locked_content
    try:
        with open(LOCKED_CONTENT_FILE, "w", encoding="utf-8") as f:
            if locked_content:
                f.write(locked_content)
            else:
                f.write("")
    except Exception as e:
        print(f"[DB ERR] saving locked content: {e}")

def load_movie_duplicate_dbs():
    global movie_fwd_unique_ids, movie_target_compound_keys
    movie_fwd_unique_ids = set()
    movie_target_compound_keys = set()
    
    if os.path.exists(DUPLICATE_DB_FILE):
        try:
            with open(DUPLICATE_DB_FILE, "r") as f:
                for line in f:
                    movie_fwd_unique_ids.add(line.strip())
        except Exception as e:
            print(f"[DB ERR] loading movie duplicate DB: {e}")
            
    if os.path.exists(TARGET_MOVIE_INDEX_DB_FILE):
        try:
            with open(TARGET_MOVIE_INDEX_DB_FILE, "r") as f:
                target_data = json.load(f)
                movie_fwd_unique_ids.update(target_data.get("unique_ids", []))
                movie_target_compound_keys.update(target_data.get("compound_keys", []))
        except Exception as e:
            print(f"[DB ERR] loading target movie index DB: {e}")

def load_webseries_duplicate_dbs():
    global webseries_fwd_unique_ids, webseries_target_compound_keys
    webseries_fwd_unique_ids = set()
    webseries_target_compound_keys = set()
    
    if os.path.exists(DUPLICATE_DB_FILE):
        try:
            with open(DUPLICATE_DB_FILE, "r") as f:
                for line in f:
                    webseries_fwd_unique_ids.add(line.strip())
        except Exception as e:
            print(f"[DB ERR] loading webseries duplicate DB: {e}")

    if os.path.exists(TARGET_WEBSERIES_INDEX_DB_FILE):
        try:
            with open(TARGET_WEBSERIES_INDEX_DB_FILE, "r") as f:
                target_data = json.load(f)
                webseries_fwd_unique_ids.update(target_data.get("unique_ids", []))
                webseries_target_compound_keys.update(target_data.get("compound_keys", []))
        except Exception as e:
            print(f"[DB ERR] loading target webseries index DB: {e}")

# NAYA: Load Full Forward DBs
def load_full_duplicate_dbs():
    global full_fwd_unique_ids, full_target_compound_keys
    full_fwd_unique_ids = set()
    full_target_compound_keys = set()
    
    if os.path.exists(DUPLICATE_DB_FILE):
        try:
            with open(DUPLICATE_DB_FILE, "r") as f:
                for line in f:
                    full_fwd_unique_ids.add(line.strip())
        except Exception as e:
            print(f"[DB ERR] loading full duplicate DB: {e}")

    if os.path.exists(FULL_TARGET_INDEX_DB_FILE):
        try:
            with open(FULL_TARGET_INDEX_DB_FILE, "r") as f:
                target_data = json.load(f)
                full_fwd_unique_ids.update(target_data.get("unique_ids", []))
                full_target_compound_keys.update(target_data.get("compound_keys", []))
        except Exception as e:
            print(f"[DB ERR] loading target full index DB: {e}")

def save_forwarded_id(unique_id, compound_key, db_type="movie"):
    try:
        with open(DUPLICATE_DB_FILE, "a") as f:
            f.write(f"{unique_id}\n")
        
        if db_type == "movie":
            movie_fwd_unique_ids.add(unique_id)
            if compound_key:
                movie_target_compound_keys.add(compound_key)
        elif db_type == "webseries":
            webseries_fwd_unique_ids.add(unique_id)
            if compound_key:
                webseries_target_compound_keys.add(compound_key)
        elif db_type == "full": # NAYA
            full_fwd_unique_ids.add(unique_id)
            if compound_key:
                full_target_compound_keys.add(compound_key)
            
    except Exception as e:
        print(f"[DB ERR] saving duplicate ID: {e}")

def load_movie_index_db():
    global movie_index
    if os.path.exists(MOVIE_INDEX_DB_FILE):
        try:
            with open(MOVIE_INDEX_DB_FILE, "r") as f:
                movie_index = json.load(f)
        except Exception as e:
            print(f"[DB ERR] loading movie index DB: {e}")
            movie_index = get_default_movie_index()
    else:
        movie_index = get_default_movie_index()

def save_movie_index_db():
    try:
        with open(MOVIE_INDEX_DB_FILE, "w") as f:
            json.dump(movie_index, f, indent=2)
    except Exception as e:
        print(f"[DB ERR] saving movie index DB: {e}")

def only_admin(_, __, m):
    return m.from_user and m.from_user.id == ADMIN_ID

def _is_invite_link(s: str) -> bool:
    return bool(re.search(r"(t\.me\/\+|joinchat\/|\?startinvite=|\?invite=)", s))

# --- resolve_chat_id फ़ंक्शन में सुधार (Final Fix) ---
async def resolve_chat_id(client: Client, ref: str | int):
    # स्ट्रिंग इनपुट को साफ करें
    ref_str = str(ref).strip()

    # चेक 1: Numeric ID (Negative या Positive)
    is_numeric_id = False
    try:
        # अगर यह एक संख्या है (जैसे -100...)
        if ref_str.lstrip('-').isdigit():
            chat_id_int = int(ref_str)
            is_numeric_id = True
            
            try:
                chat = await client.get_chat(chat_id_int)
                return chat
            except PeerIdInvalid:
                raise RuntimeError("❌ Peer ID not known. **सुनिश्चित करें कि User Account इस चैट में है या /sync चलाएं।**")
            except RPCError as e:
                raise RuntimeError(f"❌ Resolve Failed (Numeric ID): {e}")
            except Exception as e:
                # अन्य अप्रत्याशित त्रुटि को पकड़ें, लेकिन username resolution से बचें
                raise RuntimeError(f"❌ Resolve Failed (Numeric ID Internal Error): {e}")
                
    except ValueError:
        # यह संख्यात्मक ID नहीं थी, आगे बढ़ें
        pass

    # चेक 2: Invite Link
    if _is_invite_link(ref_str):
        try:
            chat = await client.join_chat(ref_str)
            return chat
        except UserAlreadyParticipant:
            chat = await client.get_chat(ref_str)
            return chat
        except (InviteHashExpired, InviteHashInvalid) as e:
            raise RuntimeError(f"❌ Invite link invalid/expired: {e}")
        except ChatAdminRequired as e:
            raise RuntimeError(f"❌ Need admin to use this invite: {e}")
        except RPCError as e:
            raise RuntimeError(f"❌ Invite link resolve failed: {e}")

    # चेक 3: Username (@channel) या अन्य स्ट्रिंग
    if isinstance(ref, str) and not is_numeric_id:
        try:
            chat = await client.get_chat(ref_str)
            return chat
        except UsernameInvalid: # विशिष्ट रूप से Invalid Username को पकड़ा गया
            raise RuntimeError(f"❌ Username invalid. **कृपया Target Chat ID या @username जांचें।**")
        except PeerIdInvalid:
            raise RuntimeError("❌ Peer not known. **Run /sync first!**")
        except RPCError as e:
            raise RuntimeError(f"❌ Resolve failed: {e}")
        
    # Fallback error
    raise RuntimeError("❌ Invalid input format. **ID (-100...) या @username का उपयोग करें।**")

# --- END resolve_chat_id फ़ंक्शन में सुधार ---

START_MESSAGE = """
**🚀 Welcome, Admin! (Full Forward Bot)**

--- **🚀 FULL FORWARD (Sab Kuch)** ---
* `/index_full <chat_id>` - Source ko scan karke `full_source_index.json` banata hai. (Old to New)
* `/index_target_full <chat_id>` - Target ko scan karke `full_target_index.json` banata hai.
* `/forward_full <target_chat_id> [limit]` - Full index se sab kuch forward karta hai.
* `/clear_full_index` - Source ka full index (`.json`) delete karta hai.
* `/clear_target_full_index` - Target ka full index (`.json`) delete karta hai.

--- **🎬 MOVIE (Alag Se)** ---
* `/index <chat_id>` - Sirf Movies ko index karta hai.
* `/index_target <chat_id>` - Sirf Movies ka target index banata hai.
* `/clear_index` - Movie index delete karta hai.
* `/clear_target_index` - Target movie index delete karta hai.
* `/start_forward` - Sirf Movies ko forward karta hai.

--- **📺 WEB SERIES (Alag Se)** ---
* `/index_webseries <chat_id>` - Sirf Web Series ko index/sort karta hai.
* `/index_target_webseries <chat_id>` - Sirf Web Series ka target index banata hai.
* `/clear_webseries_index` - Web Series index delete karta hai.
* `/clear_target_webseries_index` - Target web series index delete karta hai.
* `/forward_webseries <target_chat_id> [limit]` - Sirf Web Series ko forward karta hai.

--- **✒️ CAPTION EDITING** ---
* `/set_locked_content <text>` - Text set karein jo delete nahi hoga / add hoga.
* `/clear_locked_content` - Locked text ko hatata hai.
* `/index_for_editing <chat_id>` - **(Pehla Step)** Editing ke liye index banata hai. (Ab yeh fast hai!)
* `/clean_captions [limit]` - Captions se links/usernames hatata hai.
* `/add_locked_content [limit]` - Captions ke niche locked text add karta hai.
* `/replace_all_content [limit]` - Clean karta hai, fir add karta hai.

--- **🛠️ UTILITY COMMANDS** ---
* `/set_target <chat_id>` - (Movies ke liye) Target set karein.
* `/set_limit <number>` - (Movies ke liye) Max limit.
* `/mode <copy/forward>` - `copy` (default) ya `forward`.
* `/status` - Current status dikhata hai.
* `/sync` - Bot ke cache ko sync karta hai.
* `/stop_all` - Sabhi current tasks ko rokta hai.
* `/ping` - Bot zinda hai ya nahi.
* `/start` - Yeh help message.

--- **⚠️ DANGER ZONE** ---
* `/clean_dupes <chat_id>` - Channel se duplicate media delete karta hai.
* `/find_bad_quality <chat_id>` - Low-quality media ko index karta hai.
* `/forward_bad_quality <target_chat_id> [limit]` - Low-quality media ko forward karta hai.
"""

@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_cmd(_, message):
    await message.reply(START_MESSAGE, disable_web_page_preview=True)

def get_media_details(m):
    # Audio bhi add kar diya taki music file wali movies bhi pakad le
    media = m.video or m.document or m.audio
    if not media:
        return None, None, None
    file_name = getattr(media, 'file_name', None)
    file_size = getattr(media, 'file_size', None)
    file_unique_id = getattr(media, 'file_unique_id', None)
    return file_name, file_size, file_unique_id

# --- /index (Movies) ---
@app.on_message(filters.command("index") & filters.create(only_admin))
async def index_channel_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return
    
    try:
        source_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index -100123...` or `/index @channel`")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING, movie_index
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, source_ref)
                src_id = chat.id
                src_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            movie_index = get_default_movie_index()
            movie_index["source_channel_id"] = src_id
            movie_index["source_channel_name"] = src_name
            
            status = await message.reply(f"⏳ Movie Indexing shuru ho raha hai: `{src_name}`...\n(Yeh web series ko skip kar dega)\n(Stage 1: Videos)")

            processed_stage1 = 0
            processed_stage2 = 0
            found_count = 0
            
            try:
                # Stage 1: Videos
                async for m in app.search_messages(src_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage1 += 1
                    try:
                        file_name, file_size, unique_id = get_media_details(m)
                        if not unique_id: continue 
                        
                        text_to_check = (file_name or "") + " " + (m.caption or "")
                        if SERIES_KEYWORDS_REGEX.search(text_to_check):
                            continue
                        
                        if unique_id not in movie_index["movies"]:
                            movie_index["movies"][unique_id] = { 
                                "message_id": m.id,
                                "file_name": file_name,
                                "file_size": file_size
                            }
                            found_count += 1
                    except Exception as e: print(f"[INDEX S1 ERR] Msg {m.id}: {e}")
                    
                    if processed_stage1 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Movies... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")
                        except FloodWait: pass 
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Indexing Movies... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")

                # Stage 2: Documents (Files)
                async for m in app.search_messages(src_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break

                    processed_stage2 += 1
                    try:
                        # FIX: Removed strict video mime check to avoid skipping movie documents
                        file_name, file_size, unique_id = get_media_details(m)
                        if not unique_id: continue

                        text_to_check = (file_name or "") + " " + (m.caption or "")
                        if SERIES_KEYWORDS_REGEX.search(text_to_check):
                            continue

                        if unique_id not in movie_index["movies"]:
                            movie_index["movies"][unique_id] = { 
                                "message_id": m.id,
                                "file_name": file_name,
                                "file_size": file_size
                            }
                            found_count += 1
                    except Exception as e: print(f"[INDEX S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Movies... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {found_count} unique")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return

                save_movie_index_db()
                await status.edit(f"🎉 Movie Indexing Complete!\nChannel: `{src_name}`\nFound: **{found_count}** unique movies.\n\nDatabase ko `movie_database.json` me save kar diya hai.")

            except Exception as e:
                if status: await status.edit(f"❌ Movie Indexing Error: `{e}`")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

# --- /index_target (Movies) ---
@app.on_message(filters.command("index_target") & filters.create(only_admin))
async def index_target_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return
    
    try:
        target_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_target -100123...` or `/index_target @channel`")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, target_ref)
                tgt_id = chat.id
                tgt_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            target_unique_ids = set()
            target_compound_keys_set = set()
            
            status = await message.reply(f"⏳ Target Movie Indexing shuru ho raha hai: `{tgt_name}`...\n(Stage 1: Videos)")

            processed_stage1 = 0
            processed_stage2 = 0
            
            try:
                # Stage 1: Videos
                async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage1 += 1
                    try:
                        text_to_check = (getattr(m.video, 'file_name', "") or "") + " " + (m.caption or "")
                        if SERIES_KEYWORDS_REGEX.search(text_to_check):
                            continue
                            
                        file_name, file_size, unique_id = get_media_details(m)
                        if unique_id:
                            target_unique_ids.add(unique_id)
                        if file_name and file_size:
                            target_compound_keys_set.add(f"{file_name}-{file_size}")
                    except Exception as e: print(f"[INDEX_TGT S1 ERR] Msg {m.id}: {e}")
                    
                    if processed_stage1 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Target Movies... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")
                        except FloodWait: pass 
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Indexing Target Movies... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")

                # Stage 2: Documents (Files)
                async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break

                    processed_stage2 += 1
                    try:
                        # FIX: Removed strict video mime check
                        file_name, file_size, unique_id = get_media_details(m)
                        if not unique_id: continue
                        
                        text_to_check = (file_name or "") + " " + (m.caption or "")
                        if SERIES_KEYWORDS_REGEX.search(text_to_check):
                            continue

                        if unique_id:
                            target_unique_ids.add(unique_id)
                        if file_name and file_size:
                            target_compound_keys_set.add(f"{file_name}-{file_size}")
                    except Exception as e: print(f"[INDEX_TGT S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Target Movies... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(target_unique_ids)} unique")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return

                target_db_data = {
                    "unique_ids": list(target_unique_ids),
                    "compound_keys": list(target_compound_keys_set)
                }
                with open(TARGET_MOVIE_INDEX_DB_FILE, "w") as f:
                    json.dump(target_db_data, f)
                
                load_movie_duplicate_dbs() 
                
                await status.edit(f"🎉 Target Movie Indexing Complete!\nChannel: `{tgt_name}`\nFound: **{len(target_unique_ids)}** unique IDs.\nFound: **{len(target_compound_keys_set)}** compound keys (name+size).\n\nDuplicate list update ho gayi hai.")

            except Exception as e:
                if status: await status.edit(f"❌ Target Movie Indexing Error: `{e}`")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

def parse_series_info(text):
    text = text.replace('\n', ' ').replace('.', ' ')
    
    match = EPISODE_PACK_REGEX.search(text)
    if match:
        name = match.group(1).strip()
        season = int(match.group(2))
        ep_start = int(match.group(3))
        ep_end = int(match.group(4))
        return name, season, ep_start, ep_end

    match = EPISODE_REGEX.search(text)
    if match:
        name = match.group(1).strip()
        season = int(match.group(2))
        ep_start = int(match.group(3))
        return name, season, ep_start, None

    match = SEASON_COMPLETE_REGEX.search(text)
    if match:
        name = match.group(1).strip()
        season = int(match.group(2))
        return name, season, 999, None

    match = EPISODE_ONLY_REGEX.search(text)
    if match:
        name = match.group(1).strip()
        season = 1
        ep_start = int(match.group(2))
        return name, season, ep_start, None
        
    return None, 0, 0, 0

@app.on_message(filters.command("index_webseries") & filters.create(only_admin))
async def index_webseries_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return

    try:
        source_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_webseries -100123...` or `/index_webseries @channel`")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, source_ref)
                src_id = chat.id
                src_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            status = await message.reply(f"⏳ Web Series Indexing & Sorting shuru ho raha hai: `{src_name}`...\n(Yeh movies ko skip kar dega)\n(Stage 1: Videos)")

            processed_stage1 = 0
            processed_stage2 = 0
            found_count = 0
            temp_webseries_list = []
            
        try:
                # Single Pass: Scan Full Chat History (More Accurate)
                processed_count = 0
                
                async for m in app.get_chat_history(src_id):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_count += 1
                    
                    try:
                        # Check logic using existing helper
                        file_name, file_size, unique_id = get_media_details(m)
                        
                        # Agar media nahi hai (Text msg) ya Duplicate hai -> Skip
                        if not unique_id or unique_id in indexed_ids: 
                            # Optional: Status update every 1000 msgs even if no media found
                            if processed_count % 1000 == 0:
                                try: await status.edit(f"⏳ Scanning History...\nChecked: {processed_count} msgs\nFound: {found_count} media")
                                except FloodWait: pass
                            continue 
                        
                        temp_media_list.append({
                            "message_id": m.id,
                            "chat_id": src_id,
                            "file_name": file_name,
                            "file_size": file_size,
                            "file_unique_id": unique_id
                        })
                        indexed_ids.add(unique_id)
                        found_count += 1

                    except Exception as e: 
                        print(f"[INDEX_FULL ERR] Msg {m.id}: {e}")
                    
                    if processed_count % 500 == 0:
                        try: await status.edit(f"⏳ Scanning History...\nChecked: {processed_count} msgs\nFound: {found_count} media")
                        except FloodWait: pass 
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Indexing Web Series... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} episodes")

                # Stage 2: Documents (Files)
                async for m in app.search_messages(src_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage2 += 1
                    try:
                        # FIX: Removed strict video mime check
                        file_name, file_size, unique_id = get_media_details(m)
                        if not unique_id: continue

                        text_to_check = (file_name or "") + " " + (m.caption or "")
                        if not SERIES_KEYWORDS_REGEX.search(text_to_check):
                            continue

                        series_name, season_num, ep_start, ep_end = parse_series_info(text_to_check)
                        if not series_name:
                             if SIMPLE_SEASON_REGEX.search(text_to_check):
                                 series_name = file_name or "Unknown Series"
                                 season_num = 1
                                 ep_start = 1
                             else:
                                 continue
                        
                        temp_webseries_list.append({
                            "series_name": series_name,
                            "season_num": season_num,
                            "episode_num": ep_start,
                            "episode_end_num": ep_end,
                            "message_id": m.id,
                            "chat_id": src_id,
                            "file_name": file_name,
                            "file_size": file_size,
                            "file_unique_id": unique_id
                        })
                        found_count += 1
                    except Exception as e: print(f"[INDEX_WS S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Web Series... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {found_count} episodes")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Scan complete. Found {found_count} episodes.\nAb sorting shuru kar raha hoon (line se jama raha hoon)...")
                
                sorted_webseries_list = sorted(
                    temp_webseries_list, 
                    key=lambda x: (
                        x['series_name'].lower().strip(), 
                        x['season_num'],                  
                        x['episode_num']                  
                    )
                )
                
                if not GLOBAL_TASK_RUNNING: return

                try:
                    with open(WEBSERIES_INDEX_DB_FILE, "w", encoding="utf-8") as f:
                        json.dump(sorted_webseries_list, f, indent=2, ensure_ascii=False)
                except Exception as e:
                    await status.edit(f"❌ Database save nahi kar paaya: {e}")
                    return

                await status.edit(f"🎉 Web Series Indexing & Sorting Complete!\nChannel: `{src_name}`\nFound: **{found_count}** total episodes.\n\nSorted database ko `webseries_database.json` me save kar diya hai.")

            except Exception as e:
                if status: await status.edit(f"❌ Web Series Indexing Error: `{e}`")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

@app.on_message(filters.command("index_target_webseries") & filters.create(only_admin))
async def index_target_webseries_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return

    try:
        target_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_target_webseries -100123...` or `/index_target_webseries @channel`")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, target_ref)
                tgt_id = chat.id
                tgt_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            target_unique_ids = set()
            target_compound_keys_set = set()
            
            status = await message.reply(f"⏳ Target Web Series Indexing shuru ho raha hai: `{tgt_name}`...\n(Stage 1: Videos)")

            processed_stage1 = 0
            processed_stage2 = 0
            
            try:
                # Stage 1: Videos
                async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage1 += 1
                    try:
                        text_to_check = (getattr(m.video, 'file_name', "") or "") + " " + (m.caption or "")
                        if not SERIES_KEYWORDS_REGEX.search(text_to_check):
                            continue
                            
                        file_name, file_size, unique_id = get_media_details(m)
                        if unique_id:
                            target_unique_ids.add(unique_id)
                        if file_name and file_size:
                            target_compound_keys_set.add(f"{file_name}-{file_size}")
                    except Exception as e: print(f"[INDEX_TGT_WS S1 ERR] Msg {m.id}: {e}")
                    
                    if processed_stage1 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Target Web Series... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")
                        except FloodWait: pass 
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Indexing Target Web Series... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")

                # Stage 2: Documents (Files)
                async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break

                    processed_stage2 += 1
                    try:
                        # FIX: Removed strict video mime check
                        file_name, file_size, unique_id = get_media_details(m)
                        if not unique_id: continue
                        
                        text_to_check = (file_name or "") + " " + (m.caption or "")
                        if not SERIES_KEYWORDS_REGEX.search(text_to_check):
                            continue

                        if unique_id:
                            target_unique_ids.add(unique_id)
                        if file_name and file_size:
                            target_compound_keys_set.add(f"{file_name}-{file_size}")
                    except Exception as e: print(f"[INDEX_TGT_WS S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Target Web Series... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(target_unique_ids)} unique")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return
                
                target_db_data = {
                    "unique_ids": list(target_unique_ids),
                    "compound_keys": list(target_compound_keys_set)
                }
                with open(TARGET_WEBSERIES_INDEX_DB_FILE, "w") as f:
                    json.dump(target_db_data, f)
                
                load_webseries_duplicate_dbs() 
                
                await status.edit(f"🎉 Target Web Series Indexing Complete!\nChannel: `{tgt_name}`\nFound: **{len(target_unique_ids)}** unique IDs.\nFound: **{len(target_compound_keys_set)}** compound keys.\n\nDuplicate list update ho gayi hai.")

            except Exception as e:
                if status: await status.edit(f"❌ Target Web Series Indexing Error: `{e}`")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

# --- NAYA: Full Forward Commands ---

@app.on_message(filters.command("index_full") & filters.create(only_admin))
async def index_full_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return

    try:
        source_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_full -100123...` or `/index_full @channel`")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, source_ref)
                src_id = chat.id
                src_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            status = await message.reply(f"⏳ **Full Indexing** shuru ho raha hai: `{src_name}`...\n(Sabhi videos/files index honge)\n(Stage 1: Videos)")

            processed_stage1 = 0
            processed_stage2 = 0
            found_count = 0
            temp_media_list = []
            indexed_ids = set() # Avoid duplicates within index
            
            try:
                # Stage 1: Videos
                async for m in app.search_messages(src_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage1 += 1
                    try:
                        file_name, file_size, unique_id = get_media_details(m)
                        if not unique_id or unique_id in indexed_ids: continue 
                        
                        temp_media_list.append({
                            "message_id": m.id,
                            "chat_id": src_id,
                            "file_name": file_name,
                            "file_size": file_size,
                            "file_unique_id": unique_id
                        })
                        indexed_ids.add(unique_id)
                        found_count += 1
                    except Exception as e: print(f"[INDEX_FULL S1 ERR] Msg {m.id}: {e}")
                    
                    if processed_stage1 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing All Media... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {found_count} media")
                        except FloodWait: pass 
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Indexing All Media... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} media")

                # Stage 2: Documents (Files)
                async for m in app.search_messages(src_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage2 += 1
                    try:
                        # FIX: Removed the mime-type check that was causing missing movies
                        file_name, file_size, unique_id = get_media_details(m)
                        if not unique_id or unique_id in indexed_ids: continue
                        
                        temp_media_list.append({
                            "message_id": m.id,
                            "chat_id": src_id,
                            "file_name": file_name,
                            "file_size": file_size,
                            "file_unique_id": unique_id
                        })
                        indexed_ids.add(unique_id)
                        found_count += 1
                    except Exception as e: print(f"[INDEX_FULL S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing All Media... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {found_count} media")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Scan complete. Found {found_count} media.\nAb order reverse kar raha hoon (Old to New)...")
                
                # Reverse list to get Oldest to Newest order for forwarding
                temp_media_list.reverse()
                
                if not GLOBAL_TASK_RUNNING: return

                try:
                    with open(FULL_SOURCE_INDEX_DB_FILE, "w", encoding="utf-8") as f:
                        json.dump(temp_media_list, f, indent=2, ensure_ascii=False)
                except Exception as e:
                    await status.edit(f"❌ Database save nahi kar paaya: {e}")
                    return

                await status.edit(f"🎉 **Full Indexing Complete!**\nChannel: `{src_name}`\nFound: **{found_count}** total media files.\n\n(Old to New) database ko `full_source_index.json` me save kar diya hai.")

            except Exception as e:
                if status: await status.edit(f"❌ Full Indexing Error: `{e}`")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

@app.on_message(filters.command("index_target_full") & filters.create(only_admin))
async def index_target_full_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return

    try:
        target_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_target_full -100123...` or `/index_target_full @channel`")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, target_ref)
                tgt_id = chat.id
                tgt_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            target_unique_ids = set()
            target_compound_keys_set = set()
            
            status = await message.reply(f"⏳ **Full Target Indexing** shuru ho raha hai: `{tgt_name}`...\n(Stage 1: Videos)")

            processed_stage1 = 0
            processed_stage2 = 0
            
            try:
                # Stage 1: Videos
                async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage1 += 1
                    try:
                        file_name, file_size, unique_id = get_media_details(m)
                        if unique_id:
                            target_unique_ids.add(unique_id)
                        if file_name and file_size:
                            target_compound_keys_set.add(f"{file_name}-{file_size}")
                    except Exception as e: print(f"[INDEX_TGT_FULL S1 ERR] Msg {m.id}: {e}")
                    
                    if processed_stage1 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Full Target... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")
                        except FloodWait: pass 
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Indexing Full Target... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")

                # Stage 2: Documents (Files)
                async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break

                    processed_stage2 += 1
                    try:
                        # FIX: Removed strict video mime check
                        file_name, file_size, unique_id = get_media_details(m)
                        if not unique_id: continue
                        
                        if unique_id:
                            target_unique_ids.add(unique_id)
                        if file_name and file_size:
                            target_compound_keys_set.add(f"{file_name}-{file_size}")
                    except Exception as e: print(f"[INDEX_TGT_FULL S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing Full Target... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(target_unique_ids)} unique")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return
                
                target_db_data = {
                    "unique_ids": list(target_unique_ids),
                    "compound_keys": list(target_compound_keys_set)
                }
                with open(FULL_TARGET_INDEX_DB_FILE, "w") as f:
                    json.dump(target_db_data, f)
                
                load_full_duplicate_dbs() 
                
                await status.edit(f"🎉 **Full Target Indexing Complete!**\nChannel: `{tgt_name}`\nFound: **{len(target_unique_ids)}** unique IDs.\nFound: **{len(target_compound_keys_set)}** compound keys.\n\nFull duplicate list update ho gayi hai.")

            except Exception as e:
                if status: await status.edit(f"❌ Full Target Indexing Error: `{e}`")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())
    
# --- NAYA: /forward_full ---
@app.on_message(filters.command("forward_full") & filters.create(only_admin))
async def forward_full_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return
    
    try:
        initial_reply = await message.reply("✅ Command received. Processing...")
    except Exception as e:
        print(f"Error sending initial reply: {e}")
        return

    if not os.path.exists(FULL_SOURCE_INDEX_DB_FILE):
        await initial_reply.edit_text(f"❌ `{FULL_SOURCE_INDEX_DB_FILE}` file nahi mili. Pehle `/index_full <chat_id>` chalao.")
        return

    args = message.text.split(" ", 2)
    if len(args) < 2:
        await initial_reply.edit_text("❌ Usage:\n`/forward_full <target_chat_id> [limit]`")
        return

    target_ref = args[1].strip()
    fwd_limit = None
    if len(args) == 3:
        try:
            fwd_limit = int(args[2].strip())
        except ValueError:
            await initial_reply.edit_text("❌ Limit number hona chahiye.")
            return
            
    GLOBAL_TASK_RUNNING = True
    async def runner():
        global forwarded_count, GLOBAL_TASK_RUNNING
        status = initial_reply
        try:
            try:
                tgt_chat = await resolve_chat_id(app, target_ref)
                tgt = tgt_chat.id
                tgt_name = tgt_chat.title or tgt_chat.username
            except Exception as e:
                await status.edit_text(str(e)) 
                return

            try:
                with open(FULL_SOURCE_INDEX_DB_FILE, "r", encoding="utf-8") as f:
                    full_media_list = json.load(f)
            except Exception as e:
                await status.edit_text(f"❌ Error loading `{FULL_SOURCE_INDEX_DB_FILE}`: {e}")
                return
                
            load_full_duplicate_dbs() # Full forward ke duplicate sets load karo

            forwarded_count = 0
            duplicate_count = 0
            processed_count = 0
            
            total_in_index = len(full_media_list)
            total_to_forward_num = fwd_limit or total_in_index
            total_to_forward_str = fwd_limit or "all"

            await status.edit_text(
                f"⏳ **Full Media Forwarding** shuru ho raha hai...\n"
                f"Target: `{tgt_name}`\n"
                f"Total Media in Index: `{total_in_index}`\n"
                f"Limit: `{total_to_forward_str}`\n"
                f"Total Duplicates (Loaded): `{len(full_fwd_unique_ids)}` (IDs) + `{len(full_target_compound_keys)}` (Name+Size)",
                reply_markup=STOP_BUTTON
            )
            
            try:
                for item in full_media_list:
                    if not GLOBAL_TASK_RUNNING: break
                    
                    processed_count += 1
                    message_id = item["message_id"]
                    src_id = item["chat_id"]
                    file_name = item.get("file_name")
                    file_size = item.get("file_size")
                    unique_id = item.get("file_unique_id")
                    compound_key = f"{file_name}-{file_size}" if file_name and file_size is not None else None

                    try:
                        # Full forward ke sets check karo
                        if (unique_id and unique_id in full_fwd_unique_ids) or \
                           (compound_key and compound_key in full_target_compound_keys):
                            duplicate_count += 1
                            continue
                        
                        if mode_copy:
                            await app.copy_message(tgt, src_id, message_id)
                        else:
                            await app.forward_messages(tgt, src_id, message_id)
                        
                        save_forwarded_id(unique_id, compound_key, db_type="full") # db_type="full"
                        forwarded_count += 1
                        
                        await asyncio.sleep(PER_MSG_DELAY) 
                        
                    except FloodWait as e:
                        await status.edit_text(f"⏳ FloodWait: sleeping {e.value}s…", reply_markup=STOP_BUTTON)
                        await asyncio.sleep(e.value)
                    except (MessageIdInvalid, MessageAuthorRequired):
                        print(f"[FWD_FULL ERR] Skipping deleted/invalid msg {message_id}")
                        continue
                    except RPCError as e:
                        print(f"[FWD_FULL RPCError] Skipping msg {message_id}: {e}")
                        continue
                    except Exception as e:
                        print(f"[FWD_FULL ERROR] Skipping msg {message_id}: {e}")
                        continue
                    
                    if (forwarded_count % 50 == 0) or (processed_count % 500 == 0):
                        try:
                            await status.edit_text(
                                f"✅ Fwd: `{forwarded_count}` / {total_to_forward_num}, 🔍 Dup: `{duplicate_count}`\n"
                                f"⏳ Processed: {processed_count} / {total_in_index}",
                                reply_markup=STOP_BUTTON
                            )
                        except FloodWait: pass 

                    if forwarded_count > 0 and forwarded_count % BATCH_SIZE_FOR_BREAK == 0 and GLOBAL_TASK_RUNNING:
                        try:
                            await status.edit_text(
                                f"✅ Fwd: `{forwarded_count}`. Batch complete.\n"
                                f"☕ {BREAK_DURATION_SEC} second ka break le raha hoon...",
                                reply_markup=STOP_BUTTON
                            )
                        except FloodWait: pass
                        
                        await asyncio.sleep(BREAK_DURATION_SEC) 
                    
                    if fwd_limit and forwarded_count >= fwd_limit:
                        break

            except Exception as e:
                await status.edit_text(f"❌ Error: `{e}`", reply_markup=None)
                return
            
            final_message = f"🎉 **Full Forwarding Complete!**\n"
            if not GLOBAL_TASK_RUNNING:
                final_message = f"🛑 **Full Forwarding Stopped!**\n"
            
            await status.edit_text(
                f"{final_message}"
                f"✅ Total Forwarded: `{forwarded_count}`\n"
                f"🔍 Duplicates Skipped: `{duplicate_count}`",
                reply_markup=None
            )
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

# ------------------------------------
    
@app.on_message(filters.command("clean_dupes") & filters.create(only_admin))
async def clean_dupes_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return

    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/clean_dupes -100123...` or `/clean_dupes @channel`\n\n**Warning:** Aapka user account channel mein ADMIN (Delete Permission) hona chahiye.")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, chat_ref)
                chat_id = chat.id
                chat_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            status = await message.reply(f"⏳ **Duplicate Cleaner**\nScanning: `{chat_name}`...\n(Yeh process bohot time le sakta hai!)\n\n(Stage 1: Videos)")
            
            seen_media = {} # Naam badal diya
            messages_to_delete = []
            processed_stage1 = 0
            processed_stage2 = 0

            try:
                # Stage 1: Videos
                async for m in app.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break

                    processed_stage1 += 1
                    try:
                        file_name, file_size, unique_id = get_media_details(m)
                        if not file_name or not file_size: continue
                        
                        compound_key = f"{file_name}-{file_size}"
                        
                        if compound_key in seen_media:
                            messages_to_delete.append(m.id)
                        else:
                            seen_media[compound_key] = m.id
                    except Exception as e: print(f"[CLEAN S1 ERR] Msg {m.id}: {e}")
                    
                    if processed_stage1 % 500 == 0:
                        try: await status.edit(f"⏳ Scanning... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {len(messages_to_delete)} duplicates")
                        except FloodWait: pass

                if not GLOBAL_TASK_RUNNING: return

                # Stage 2: Documents
                await status.edit(f"⏳ Scanning... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(messages_to_delete)} duplicates")
                
                async for m in app.search_messages(chat_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break

                    processed_stage2 += 1
                    try:
                        # FIX: Removed mime-type check for cleaning dupes
                        file_name, file_size, unique_id = get_media_details(m)
                        if not file_name or not file_size: continue
                        
                        compound_key = f"{file_name}-{file_size}"
                        
                        if compound_key in seen_media:
                            messages_to_delete.append(m.id)
                        else:
                            seen_media[compound_key] = m.id
                    except Exception as e: print(f"[CLEAN S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Scanning... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(messages_to_delete)} duplicates")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return

                # Deletion Phase
                total_to_delete = len(messages_to_delete)
                if total_to_delete == 0:
                    await status.edit("🎉 Scan complete. Koi 'Name+Size' duplicate nahi mila!")
                    return

                await status.edit(f"✅ Scan complete.\nFound **{total_to_delete}** duplicates.\nAb 100 ke batch me delete karna shuru kar raha hoon...")
                
                deleted_count = 0
                batches = [messages_to_delete[i:i + 100] for i in range(0, total_to_delete, 100)]
                
                for i, batch in enumerate(batches):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Deletion stopped by user.")
                        break
                    
                    try:
                        await app.delete_messages(chat_id, batch)
                        deleted_count += len(batch)
                        await status.edit(f"🗑️ Deleting duplicates...\nBatch {i+1}/{len(batches)} done.\nTotal Deleted: {deleted_count}/{total_to_delete}")
                        await asyncio.sleep(2) 
                    except FloodWait as e:
                        await status.edit(f"⏳ FloodWait: Deleting batch {i+1}...\nSleeping for {e.value}s.")
                        await asyncio.sleep(e.value)
                        try:
                            await app.delete_messages(chat_id, batch)
                            deleted_count += len(batch)
                            await status.edit(f"🗑️ Deleting duplicates... (Retry)\nBatch {i+1}/{len(batches)} done.\nTotal Deleted: {deleted_count}/{total_to_delete}")
                        except Exception as e:
                             await status.edit(f"❌ Error deleting batch {i+1} (retry): {e}\nSkipping batch.")
                    except Exception as e:
                        await status.edit(f"❌ Error deleting batch {i+1}: {e}\nSkipping batch.")
                
                if GLOBAL_TASK_RUNNING:
                    await status.edit(f"🎉 Cleanup Complete!\nDeleted {deleted_count} duplicate media from `{chat_name}`.")
            
            except ChatAdminRequired:
                await status.edit("❌ **Error: Main Admin nahi hoon!**\nMujhe channel/group mein 'Delete Messages' permission ke saath Admin banao.")
            except Exception as e:
                if status: await status.edit(f"❌ Error during scan/delete process: {e}")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

@app.on_message(filters.command("find_bad_quality") & filters.create(only_admin))
async def find_bad_quality_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return
    
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/find_bad_quality -100123...` or `/find_bad_quality @channel`")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, chat_ref)
                chat_id = chat.id
                chat_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            status = await message.reply(f"⏳ **Bad Quality Finder**\nScanning `{chat_name}` for low-quality keywords...\n(Stage 1: Videos)")
            
            bad_quality_movies_list = []
            processed_stage1 = 0
            processed_stage2 = 0

            try:
                # Stage 1: Videos
                async for m in app.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break

                    processed_stage1 += 1
                    try:
                        file_name, file_size, unique_id = get_media_details(m)
                        text_to_check = (file_name or "") + " " + (m.caption or "")
                        
                        if BAD_QUALITY_REGEX.search(text_to_check):
                            bad_quality_movies_list.append({
                                "chat_id": chat_id,
                                "message_id": m.id,
                                "file_name": file_name,
                                "file_size": file_size,
                                "caption": m.caption,
                                "unique_id": unique_id
                            })
                    except Exception as e: print(f"[FIND BQ S1 ERR] Msg {m.id}: {e}")
                    
                    if processed_stage1 % 500 == 0:
                        try: await status.edit(f"⏳ Scanning... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {len(bad_quality_movies_list)} bad quality movies")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Scanning... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(bad_quality_movies_list)} bad quality movies")
                
                # Stage 2: Documents
                async for m in app.search_messages(chat_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage2 += 1
                    try:
                        # FIX: Removed mime-type check
                        file_name, file_size, unique_id = get_media_details(m)
                        text_to_check = (file_name or "") + " " + (m.caption or "")
                        
                        if BAD_QUALITY_REGEX.search(text_to_check):
                            bad_quality_movies_list.append({
                                "chat_id": chat_id,
                                "message_id": m.id,
                                "file_name": file_name,
                                "file_size": file_size,
                                "caption": m.caption,
                                "unique_id": unique_id
                            })
                    except Exception as e: print(f"[FIND BQ S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Scanning... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(bad_quality_movies_list)} bad quality movies")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return

                with open(BAD_QUALITY_DB_FILE, "w", encoding="utf-8") as f:
                    json.dump(bad_quality_movies_list, f, indent=2, ensure_ascii=False)
                
                await status.delete()
                if bad_quality_movies_list:
                    await message.reply_document(
                        BAD_QUALITY_DB_FILE,
                        caption=f"🎉 **Bad Quality Scan Complete!**\nChannel: `{chat_name}`\nFound **{len(bad_quality_movies_list)}** potentially low-quality audio movies.\n\nFile: `bad_quality_movies.json`"
                    )
                else:
                    await message.reply(f"🎉 Scan complete. Koi low-quality audio movie nahi mili (keywords ke hisaab se) `{chat_name}` mein.")

            except Exception as e:
                if status: await status.edit(f"❌ Error during bad quality scan: {e}")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

@app.on_message(filters.command("forward_bad_quality") & filters.create(only_admin))
async def forward_bad_quality_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return

    if not os.path.exists(BAD_QUALITY_DB_FILE):
        await message.reply("❌ `bad_quality_movies.json` file nahi mili. Pehle `/find_bad_quality <chat_id>` chalao.")
        return

    args = message.text.split(" ", 2)
    if len(args) < 2:
        await message.reply("❌ Usage:\n`/forward_bad_quality <target_chat_id> [limit]`")
        return

    target_ref = args[1].strip()
    forward_limit = None
    if len(args) == 3:
        try:
            forward_limit = int(args[2].strip())
        except ValueError:
            await message.reply("❌ Limit number hona chahiye.")
            return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING, forwarded_count
        status = None
        try:
            try:
                target_chat = await resolve_chat_id(app, target_ref)
                tgt_id = target_chat.id
                tgt_name = target_chat.title or target_chat.username
            except RuntimeError as e: # resolve_chat_id से आने वाले errors
                await message.reply(f"❌ Target Chat Resolve Error: {str(e)}")
                return
            except Exception as e:
                # Fallback for unexpected errors during chat resolution
                await message.reply(f"❌ Target Chat Resolve Failed: {e}")
                return
            
            try:
                with open(BAD_QUALITY_DB_FILE, "r", encoding="utf-8") as f:
                    bad_quality_movies_data = json.load(f)
            except Exception as e:
                await message.reply(f"❌ Error loading `bad_quality_movies.json`: {e}")
                return

            if not bad_quality_movies_data:
                await message.reply("ℹ️ `bad_quality_movies.json` mein koi movie nahi mili forward karne ke liye.")
                return

            total_to_forward = len(bad_quality_movies_data)
            if forward_limit:
                bad_quality_movies_data = bad_quality_movies_data[:forward_limit]
                total_to_forward = min(total_to_forward, forward_limit)

            status = await message.reply(
                f"⏳ **Bad Quality Movies Forwarder**\n"
                f"Target: `{tgt_name}`\n"
                f"Total to Forward: `{total_to_forward}`\n"
                f"Forwarding in 50-movie batches...\n\n"
                f"Progress: 0/{total_to_forward}",
                reply_markup=STOP_BUTTON
            )
            
            forwarded_count = 0
            batch_counter = 0

            for movie in bad_quality_movies_data:
                if not GLOBAL_TASK_RUNNING:
                    await status.edit("🛑 Task stopped by user.", reply_markup=None)
                    break
                
                if forwarded_count >= total_to_forward and forward_limit:
                    break
                
                src_id = movie["chat_id"]
                message_id = movie["message_id"]

                try:
                    await app.copy_message(tgt_id, src_id, message_id)
                    forwarded_count += 1
                    batch_counter += 1
                    
                    await asyncio.sleep(0.5) 
                    
                    if batch_counter % 50 == 0:
                        try:
                            await status.edit(f"⏳ Forwarding bad quality movies...\nProgress: {forwarded_count}/{total_to_forward}\nTaking a short break (5s)...", reply_markup=STOP_BUTTON)
                            await asyncio.sleep(5) 
                            await status.edit(f"⏳ Forwarding bad quality movies...\nProgress: {forwarded_count}/{total_to_forward}", reply_markup=STOP_BUTTON)
                        except FloodWait: pass 

                except FloodWait as e:
                    await status.edit(f"⏳ FloodWait: sleeping {e.value}s…", reply_markup=STOP_BUTTON)
                    await asyncio.sleep(e.value)
                    try:
                        await app.copy_message(tgt_id, src_id, message_id)
                        forwarded_count += 1
                        batch_counter += 1
                    except Exception as retry_e:
                        print(f"[FWD BQ RETRY ERR] Msg {message_id}: {retry_e}")
                except (MessageIdInvalid, MessageAuthorRequired):
                    print(f"[FWD BQ ERR] Skipping deleted/invalid msg {message_id}")
                    continue
                except RPCError as e:
                    print(f"[FWD BQ RPCError] Skipping msg {message_id}: {e}")
                    continue
                except Exception as e:
                    print(f"[FWD BQ ERROR] Skipping msg {message_id}: {e}")
                    continue
                
                try:
                    if forwarded_count % 10 == 0: 
                        await status.edit(f"⏳ Forwarding bad quality movies...\nProgress: {forwarded_count}/{total_to_forward}", reply_markup=STOP_BUTTON)
                except FloodWait: pass

            if GLOBAL_TASK_RUNNING:
                await status.edit(
                    f"🎉 **Bad Quality Movies Forwarding Complete!**\n"
                    f"Total Forwarded: `{forwarded_count}` to `{tgt_name}`.",
                    reply_markup=None
                )
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

# --- Clear Commands ---
@app.on_message(filters.command("clear_index") & filters.create(only_admin))
async def clear_index_cmd(_, message):
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot busy hai, abhi index clear nahi kar sakta.")
        return
    global movie_index
    try:
        if os.path.exists(MOVIE_INDEX_DB_FILE):
            os.remove(MOVIE_INDEX_DB_FILE)
            movie_index = get_default_movie_index()
            await message.reply(f"✅ Source movie index (`{MOVIE_INDEX_DB_FILE}`) delete kar diya hai.")
        else:
            await message.reply(f"ℹ️ Source movie index pehle se hi khaali hai.")
    except Exception as e:
        await message.reply(f"❌ Source movie index delete nahi kar paaya: {e}")

@app.on_message(filters.command("clear_target_index") & filters.create(only_admin))
async def clear_target_index_cmd(_, message):
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot busy hai, abhi index clear nahi kar sakta.")
        return
    global movie_target_compound_keys
    try:
        if os.path.exists(TARGET_MOVIE_INDEX_DB_FILE):
            os.remove(TARGET_MOVIE_INDEX_DB_FILE)
            load_movie_duplicate_dbs() 
            movie_target_compound_keys = set() 
            await message.reply(f"✅ Target movie index (`{TARGET_MOVIE_INDEX_DB_FILE}`) delete kar diya hai.")
        else:
            await message.reply(f"ℹ️ Target movie index pehle se hi khaali hai.")
    except Exception as e:
        await message.reply(f"❌ Target movie index delete nahi kar paaya: {e}")

@app.on_message(filters.command("clear_webseries_index") & filters.create(only_admin))
async def clear_webseries_index_cmd(_, message):
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot busy hai, abhi index clear nahi kar sakta.")
        return
    try:
        if os.path.exists(WEBSERIES_INDEX_DB_FILE):
            os.remove(WEBSERIES_INDEX_DB_FILE)
            await message.reply(f"✅ Source webseries index (`{WEBSERIES_INDEX_DB_FILE}`) delete kar diya hai.")
        else:
            await message.reply(f"ℹ️ Source webseries index pehle se hi khaali hai.")
    except Exception as e:
        await message.reply(f"❌ Source webseries index delete nahi kar paaya: {e}")

@app.on_message(filters.command("clear_target_webseries_index") & filters.create(only_admin))
async def clear_target_webseries_index_cmd(_, message):
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot busy hai, abhi index clear nahi kar sakta.")
        return
    global webseries_target_compound_keys
    try:
        if os.path.exists(TARGET_WEBSERIES_INDEX_DB_FILE):
            os.remove(TARGET_WEBSERIES_INDEX_DB_FILE)
            load_webseries_duplicate_dbs() 
            webseries_target_compound_keys = set() 
            await message.reply(f"✅ Target webseries index (`{TARGET_WEBSERIES_INDEX_DB_FILE}`) delete kar diya hai.")
        else:
            await message.reply(f"ℹ️ Target webseries index pehle se hi khaali hai.")
    except Exception as e:
        await message.reply(f"❌ Target webseries index delete nahi kar paaya: {e}")

# NAYA: Clear Full Forward
@app.on_message(filters.command("clear_full_index") & filters.create(only_admin))
async def clear_full_index_cmd(_, message):
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot busy hai, abhi index clear nahi kar sakta.")
        return
    try:
        if os.path.exists(FULL_SOURCE_INDEX_DB_FILE):
            os.remove(FULL_SOURCE_INDEX_DB_FILE)
            await message.reply(f"✅ Source full index (`{FULL_SOURCE_INDEX_DB_FILE}`) delete kar diya hai.")
        else:
            await message.reply(f"ℹ️ Source full index pehle se hi khaali hai.")
    except Exception as e:
        await message.reply(f"❌ Source full index delete nahi kar paaya: {e}")

@app.on_message(filters.command("clear_target_full_index") & filters.create(only_admin))
async def clear_target_full_index_cmd(_, message):
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot busy hai, abhi index clear nahi kar sakta.")
        return
    global full_target_compound_keys
    try:
        if os.path.exists(FULL_TARGET_INDEX_DB_FILE):
            os.remove(FULL_TARGET_INDEX_DB_FILE)
            load_full_duplicate_dbs() 
            full_target_compound_keys = set() 
            await message.reply(f"✅ Target full index (`{FULL_TARGET_INDEX_DB_FILE}`) delete kar diya hai.")
        else:
            await message.reply(f"ℹ️ Target full index pehle se hi khaali hai.")
    except Exception as e:
        await message.reply(f"❌ Target full index delete nahi kar paaya: {e}")


# --- Utility Commands ---
@app.on_message(filters.command("set_target") & filters.create(only_admin))
async def set_target(_, message):
    global target_channel
    try:
        target_channel = message.text.split(" ", 1)[1].strip()
        await message.reply(f"✅ (Movie) Target set: `{target_channel}`")
    except:
        await message.reply("❌ Usage:\n`/set_target -100123...` or `/set_target @channel`")

@app.on_message(filters.command("set_limit") & filters.create(only_admin))
async def set_limit(_, message):
    global limit_messages
    try:
        limit_messages = int(message.text.split(" ", 1)[1].strip())
        await message.reply(f"✅ (Movie) Limit set: `{limit_messages}`")
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

# --- Stop Button & Command ---
STOP_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Task", callback_data="stop_task")]])

@app.on_callback_query(filters.regex("^stop_task$"))
async def cb_stop_task(client, query):
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ Not allowed!", show_alert=True)
        return
        
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try: await query.message.edit_text("🛑 Stop requested. Finishing current batch...", reply_markup=None)
    except: pass
    
@app.on_message(filters.command("stop_all") & filters.create(only_admin))
async def stop_all_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    GLOBAL_TASK_RUNNING = False
    await message.reply("🛑 **Universal Stop Signal Sent!**\nSabhi current tasks (indexing, forwarding, editing) agle batch ke baad ruk jayenge.")

# --- /forward_webseries ---
@app.on_message(filters.command("forward_webseries") & filters.create(only_admin))
async def forward_webseries_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return
    
    try:
        initial_reply = await message.reply("✅ Command received. Processing...")
    except Exception as e:
        print(f"Error sending initial reply: {e}")
        return

    if not os.path.exists(WEBSERIES_INDEX_DB_FILE):
        await initial_reply.edit_text("❌ `webseries_database.json` file nahi mili. Pehle `/index_webseries <chat_id>` chalao.")
        return

    args = message.text.split(" ", 2)
    if len(args) < 2:
        await initial_reply.edit_text("❌ Usage:\n`/forward_webseries <target_chat_id> [limit]`")
        return

    target_ref = args[1].strip()
    fwd_limit = None
    if len(args) == 3:
        try:
            fwd_limit = int(args[2].strip())
        except ValueError:
            await initial_reply.edit_text("❌ Limit number hona chahiye.")
            return
            
    GLOBAL_TASK_RUNNING = True
    async def runner():
        global forwarded_count, GLOBAL_TASK_RUNNING
        status = initial_reply
        try:
            try:
                tgt_chat = await resolve_chat_id(app, target_ref)
                tgt = tgt_chat.id
                tgt_name = tgt_chat.title or tgt_chat.username
            except Exception as e:
                await status.edit_text(str(e)) 
                return

            try:
                with open(WEBSERIES_INDEX_DB_FILE, "r", encoding="utf-8") as f:
                    webseries_list = json.load(f)
            except Exception as e:
                await status.edit_text(f"❌ Error loading `webseries_database.json`: {e}")
                return
                
            load_webseries_duplicate_dbs()

            forwarded_count = 0
            duplicate_count = 0
            processed_count = 0
            
            total_in_index = len(webseries_list)
            total_to_forward_num = fwd_limit or total_in_index
            total_to_forward_str = fwd_limit or "all"

            await status.edit_text(
                f"⏳ **Sorted Web Series Forwarding** shuru ho raha hai...\n"
                f"Target: `{tgt_name}`\n"
                f"Total Episodes in Index: `{total_in_index}`\n"
                f"Limit: `{total_to_forward_str}`\n"
                f"Total Duplicates (Loaded): `{len(webseries_fwd_unique_ids)}` (IDs) + `{len(webseries_target_compound_keys)}` (Name+Size)",
                reply_markup=STOP_BUTTON
            )
            
            try:
                for item in webseries_list:
                    if not GLOBAL_TASK_RUNNING: break
                    
                    processed_count += 1
                    message_id = item["message_id"]
                    src_id = item["chat_id"]
                    file_name = item.get("file_name")
                    file_size = item.get("file_size")
                    unique_id = item.get("file_unique_id")
                    compound_key = f"{file_name}-{file_size}" if file_name and file_size is not None else None

                    try:
                        if (unique_id and unique_id in webseries_fwd_unique_ids) or \
                           (compound_key and compound_key in webseries_target_compound_keys):
                            duplicate_count += 1
                            continue
                        
                        if mode_copy:
                            await app.copy_message(tgt, src_id, message_id)
                        else:
                            await app.forward_messages(tgt, src_id, message_id)
                        
                        save_forwarded_id(unique_id, compound_key, db_type="webseries") 
                        forwarded_count += 1
                        
                        await asyncio.sleep(PER_MSG_DELAY) 
                        
                    except FloodWait as e:
                        await status.edit_text(f"⏳ FloodWait: sleeping {e.value}s…", reply_markup=STOP_BUTTON)
                        await asyncio.sleep(e.value)
                    except (MessageIdInvalid, MessageAuthorRequired):
                        print(f"[FWD_WS ERR] Skipping deleted/invalid msg {message_id}")
                        continue
                    except RPCError as e:
                        print(f"[FWD_WS RPCError] Skipping msg {message_id}: {e}")
                        continue
                    except Exception as e:
                        print(f"[FWD_WS ERROR] Skipping msg {message_id}: {e}")
                        continue
                    
                    if (forwarded_count % 50 == 0) or (processed_count % 500 == 0):
                        try:
                            await status.edit_text(
                                f"✅ Fwd: `{forwarded_count}` / {total_to_forward_num}, 🔍 Dup: `{duplicate_count}`\n"
                                f"⏳ Processed: {processed_count} / {total_in_index}",
                                reply_markup=STOP_BUTTON
                            )
                        except FloodWait: pass 

                    if forwarded_count > 0 and forwarded_count % BATCH_SIZE_FOR_BREAK == 0 and GLOBAL_TASK_RUNNING:
                        try:
                            await status.edit_text(
                                f"✅ Fwd: `{forwarded_count}`. Batch complete.\n"
                                f"☕ {BREAK_DURATION_SEC} second ka break le raha hoon...",
                                reply_markup=STOP_BUTTON
                            )
                        except FloodWait: pass
                        
                        await asyncio.sleep(BREAK_DURATION_SEC) 
                    
                    if fwd_limit and forwarded_count >= fwd_limit:
                        break

            except Exception as e:
                await status.edit_text(f"❌ Error: `{e}`", reply_markup=None)
                return
            
            final_message = f"🎉 **Web Series Forwarding Complete!**\n"
            if not GLOBAL_TASK_RUNNING:
                final_message = f"🛑 **Web Series Forwarding Stopped!**\n"
            
            await status.edit_text(
                f"{final_message}"
                f"✅ Total Forwarded: `{forwarded_count}`\n"
                f"🔍 Duplicates Skipped: `{duplicate_count}`",
                reply_markup=None
            )
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

# --- /start_forward ---
@app.on_message(filters.command("start_forward") & filters.create(only_admin))
async def start_forward(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return

    try:
        initial_reply = await message.reply("✅ Command received. Processing...")
    except Exception as e:
        print(f"Error sending initial reply: {e}")
        return
        
    GLOBAL_TASK_RUNNING = True
    async def runner():
        global forwarded_count, GLOBAL_TASK_RUNNING
        status = initial_reply
        try:
            if not target_channel:
                await status.edit_text("⚠ (Movie) Pehle `/set_target` set karo.")
                return
                
            if not movie_index["source_channel_id"] or not movie_index["movies"]:
                await status.edit_text("⚠ Movie index khaali hai. Pehle `/index <channel_id>` chalao.")
                return

            try:
                tgt_chat = await resolve_chat_id(app, target_channel)
                tgt = tgt_chat.id
                src = movie_index["source_channel_id"]
                src_name = movie_index["source_channel_name"]
            except Exception as e:
                await status.edit_text(str(e))
                return
            
            load_movie_duplicate_dbs()

            forwarded_count = 0
            duplicate_count = 0
            processed_count = 0
            total_to_forward = len(movie_index["movies"])
            total_to_forward_str = limit_messages or "all"

            await status.edit_text(
                f"⏳ **Movie Forwarding** shuru ho raha hai...\n"
                f"Source (Cache): `{src_name}`\n"
                f"Target: `{tgt_chat.title or tgt_chat.username}`\n"
                f"Total Movies in Index: `{total_to_forward}`\n"
                f"Limit: `{total_to_forward_str}`\n"
                f"Total Duplicates (Loaded): `{len(movie_fwd_unique_ids)}` (IDs) + `{len(movie_target_compound_keys)}` (Name+Size)",
                reply_markup=STOP_BUTTON
            )
            
            movies_list = list(movie_index["movies"].items())
            
            try:
                for unique_id, data in movies_list:
                    if not GLOBAL_TASK_RUNNING: break
                    
                    processed_count += 1
                    message_id = data["message_id"]
                    file_name = data.get("file_name")
                    file_size = data.get("file_size")
                    compound_key = f"{file_name}-{file_size}" if file_name and file_size is not None else None

                    try:
                        if (unique_id and unique_id in movie_fwd_unique_ids) or \
                           (compound_key and compound_key in movie_target_compound_keys):
                            duplicate_count += 1
                            continue
                        
                        if mode_copy:
                            await app.copy_message(tgt, src, message_id)
                        else:
                            await app.forward_messages(tgt, src, message_id)
                        
                        save_forwarded_id(unique_id, compound_key, db_type="movie") 
                        forwarded_count += 1
                        
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
                    
                    if (forwarded_count % 50 == 0) or (processed_count % 500 == 0):
                        try:
                            await status.edit_text(
                                f"✅ Fwd: `{forwarded_count}` / {total_to_forward_str}, 🔍 Dup: `{duplicate_count}`\n"
                                f"⏳ Processed: {processed_count} / {total_to_forward}",
                                reply_markup=STOP_BUTTON
                            )
                        except FloodWait: pass 

                    if forwarded_count > 0 and forwarded_count % BATCH_SIZE_FOR_BREAK == 0 and GLOBAL_TASK_RUNNING:
                        try:
                            await status.edit_text(
                                f"✅ Fwd: `{forwarded_count}`. Batch complete.\n"
                                f"☕ {BREAK_DURATION_SEC} second ka break le raha hoon...",
                                reply_markup=STOP_BUTTON
                            )
                        except FloodWait: pass
                        
                        await asyncio.sleep(BREAK_DURATION_SEC) 
                    
                    if limit_messages and forwarded_count >= limit_messages:
                        break

            except Exception as e:
                await status.edit_text(f"❌ Error: `{e}`", reply_markup=None)
                return

            final_message = f"🎉 **Movie Forwarding Complete!**\n"
            if not GLOBAL_TASK_RUNNING:
                final_message = f"🛑 **Movie Forwarding Stopped!**\n"

            await status.edit_text(
                f"{final_message}"
                f"✅ Total Movies Forwarded: `{forwarded_count}`\n"
                f"🔍 Duplicates Skipped: `{duplicate_count}`",
                reply_markup=None
            )
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())

# --- Caption Editing Commands ---

@app.on_message(filters.command("set_locked_content") & filters.create(only_admin))
async def set_locked_content_cmd(_, message):
    global locked_content
    try:
        locked_content = message.text.split(" ", 1)[1].strip()
        save_locked_content()
        await message.reply(f"✅ Locked content set:\n\n`{locked_content}`")
    except:
        await message.reply("❌ Usage: `/set_locked_content <text to lock/add>`\n(Example: `/set_locked_content \n\nJoin: @MyChannel`)")

@app.on_message(filters.command("clear_locked_content") & filters.create(only_admin))
async def clear_locked_content_cmd(_, message):
    global locked_content
    locked_content = None
    save_locked_content()
    await message.reply("✅ Locked content clear kar diya hai.")

# --- YEH FUNCTION UPDATE KIYA GAYA HAI ---
@app.on_message(filters.command("index_for_editing") & filters.create(only_admin))
async def index_for_editing_cmd(_, message):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return

    try:
        source_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_for_editing -100123...` or `/index_for_editing @channel`")
        return

    GLOBAL_TASK_RUNNING = True
    async def runner():
        global GLOBAL_TASK_RUNNING
        status = None
        try:
            try:
                chat = await resolve_chat_id(app, source_ref)
                src_id = chat.id
                src_name = chat.title or chat.username
            except Exception as e:
                await message.reply(str(e))
                return
            
            # Locked content ko load karo taaki check kar sakein
            load_locked_content()
            
            status_text = f"⏳ **Editing Indexer** shuru ho raha hai: `{src_name}`...\n(Sirf caption wale messages index honge)\n(Stage 1: Videos)"
            if locked_content:
                status_text += f"\n\nℹ️ **Note:** Jin captions me `{locked_content[:30]}...` pehle se hai, unhe skip kar diya jayega."
            
            status = await message.reply(status_text)

            processed_stage1 = 0
            processed_stage2 = 0
            found_count = 0
            skipped_due_to_lock = 0 # Kitne skip kiye, uska count
            message_ids_with_captions = []
            
            try:
                # Stage 1: Videos
                async for m in app.search_messages(src_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break
                    
                    processed_stage1 += 1
                    try:
                        if m.caption:
                            # --- YEH HAI FIX ---
                            # Agar locked content set hai AUR woh caption me pehle se hai, toh skip karo
                            if locked_content and locked_content in m.caption:
                                skipped_due_to_lock += 1
                                continue # Skip karo, pehle se hai
                            # --- END FIX ---
                                
                            message_ids_with_captions.append(m.id)
                            found_count += 1
                    except Exception as e: print(f"[INDEX_EDIT S1 ERR] Msg {m.id}: {e}")
                    
                    if processed_stage1 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing for Editing... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {found_count} (To Edit)\nSkipped (Already OK): {skipped_due_to_lock}")
                        except FloodWait: pass 
                
                if not GLOBAL_TASK_RUNNING: return

                await status.edit(f"⏳ Indexing for Editing... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} (To Edit)\nSkipped: {skipped_due_to_lock}")

                # Stage 2: Documents (Files)
                async for m in app.search_messages(src_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                    if not GLOBAL_TASK_RUNNING:
                        await status.edit("🛑 Task stopped by user.")
                        break

                    processed_stage2 += 1
                    try:
                        # FIX: Removed mime-type check for editing index
                        if m.caption:
                            # Agar locked content set hai AUR woh caption me pehle se hai, toh skip karo
                            if locked_content and locked_content in m.caption:
                                skipped_due_to_lock += 1
                                continue # Skip karo, pehle se hai
                                
                            message_ids_with_captions.append(m.id)
                            found_count += 1
                    except Exception as e: print(f"[INDEX_EDIT S2 ERR] Msg {m.id}: {e}")

                    if processed_stage2 % 500 == 0:
                        try: await status.edit(f"⏳ Indexing for Editing... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {found_count} (To Edit)\nSkipped: {skipped_due_to_lock}")
                        except FloodWait: pass
                
                if not GLOBAL_TASK_RUNNING: return
                
                # BUG FIX: Removed .reverse() - Ab naye se purane save honge
                
                edit_index_data = {
                    "chat_id": src_id,
                    "chat_name": src_name,
                    "message_ids": message_ids_with_captions
                }
                
                with open(EDITING_INDEX_DB_FILE, "w", encoding="utf-8") as f:
                    json.dump(edit_index_data, f, indent=2)

                await status.edit(f"🎉 Editing Indexing Complete!\nChannel: `{src_name}`\n\nFound: **{found_count}** messages (jinko edit karna hai).\nSkipped: **{skipped_due_to_lock}** messages (jinke paas locked content pehle se tha).\n\nDatabase ko `{EDITING_INDEX_DB_FILE}` me save kar diya hai.")

            except Exception as e:
                if status: await status.edit(f"❌ Editing Indexing Error: `{e}`")
        finally:
            GLOBAL_TASK_RUNNING = False

    app.loop.create_task(runner())


# --- SPEED OPTIMIZED BATCH EDITOR ---

async def edit_caption_task(chat_id, msg_id, final_caption):
    """
    Helper function to edit a single caption.
    Returns: "success", "skipped", or an Exception object.
    """
    try:
        # Agar final_caption khaali hai, toh ". " use karo (telegram empty caption allow nahi karta)
        await app.edit_message_caption(chat_id, msg_id, caption=final_caption if final_caption else ". ")
        if not final_caption:
            # Ab ". " ko hata kar poora empty karo
            await app.edit_message_caption(chat_id, msg_id, caption="")
        return "success"
    except FloodWait as e:
        print(f"[EDIT FLOODWAIT] Msg {msg_id}: Sleeping {e.value}s...")
        await asyncio.sleep(e.value + 1)
        try:
            # Retry once
            await app.edit_message_caption(chat_id, msg_id, caption=final_caption if final_caption else ". ")
            if not final_caption:
                await app.edit_message_caption(chat_id, msg_id, caption="")
            return "success"
        except Exception as e_retry:
            print(f"[EDIT RETRY ERR] Msg {msg_id}: {e_retry}")
            return e_retry
    except Exception as e:
        error_text = str(e)
        if "MESSAGE_NOT_MODIFIED" in error_text or "CAPTION_EMPTY" in error_text:
            # Agar caption pehle se hi wahi tha, ya khaali tha
            return "skipped"
        print(f"[EDIT ERR] Msg {msg_id}: {e}")
        return e

async def batch_edit_captions(message, edit_mode):
    global GLOBAL_TASK_RUNNING
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot pehle se hi ek task chala raha hai. Pehle `/stop_all` use karein ya wait karein.")
        return
    
    status = None
    try:
        if not os.path.exists(EDITING_INDEX_DB_FILE):
            await message.reply(f"❌ `editing_index.json` file nahi मिली. Pehle `/index_for_editing <chat_id>` chalao.")
            return
            
        if edit_mode != "clean" and not locked_content:
            await message.reply("❌ Locked content set nahi hai! Pehle `/set_locked_content <text>` chalao.")
            return
            
        try:
            with open(EDITING_INDEX_DB_FILE, "r", encoding="utf-8") as f:
                index_data = json.load(f)
        except Exception as e:
            await message.reply(f"❌ `editing_index.json` file load nahi kar paaya: {e}")
            return

        chat_id = index_data.get("chat_id")
        chat_name = index_data.get("chat_name", "N/A")
        message_ids = index_data.get("message_ids", [])
        
        if not chat_id or not message_ids:
            await message.reply(f"❌ `editing_index.json` file corrupt hai ya khaali hai.")
            return

        GLOBAL_TASK_RUNNING = True
        limit = None
        try:
            limit_str = message.text.split(" ", 1)[1].strip()
            limit = int(limit_str)
        except:
            limit = None

        if limit:
            message_ids = message_ids[:limit]
            
        total_messages = len(message_ids)
        if total_messages == 0:
            await message.reply("ℹ️ Index me koi message ID nahi mili process karne ke liye. (Shayad sab pehle se hi correct hain?)")
            GLOBAL_TASK_RUNNING = False
            return

        status = await message.reply(
            f"⏳ **Caption Editor** (Mode: `{edit_mode.upper()}`)\n"
            f"Channel: `{chat_name}` (ID: `{chat_id}`)\n"
            f"Total Messages to process: `{total_messages}`\n"
            f"SPEED MODE: Operation 100 ke batch me concurrently shuru ho raha hai...",
            reply_markup=STOP_BUTTON
        )

        processed_count = 0
        edited_count = 0
        skipped_count = 0
        failed_count = 0
        first_error_message = None
        
        batches = [message_ids[i:i + 100] for i in range(0, total_messages, 100)]
        total_batches = len(batches)

        try:
            for i, batch in enumerate(batches):
                if not GLOBAL_TASK_RUNNING:
                    await status.edit("🛑 Task stopped by user.", reply_markup=None)
                    break
                
                try:
                    # Batch me saare messages ek saath fetch karo
                    messages_in_batch = await app.get_messages(chat_id, batch)
                    # Unhe ek dictionary me daalo taaki fast access ho sake
                    messages_dict = {msg.id: msg for msg in messages_in_batch if msg.caption is not None}
                except Exception as e:
                    await status.edit(f"❌ Batch {i+1} fetch karne me error: {e}\nBatch skip kar raha hoon.")
                    failed_count += len(batch)
                    processed_count += len(batch)
                    if first_error_message is None:
                        first_error_message = str(e)
                    continue
                
                tasks_to_run = []
                batch_processed_count = 0
                
                for msg_id in batch:
                    if not GLOBAL_TASK_RUNNING: break
                    
                    batch_processed_count += 1
                    
                    # Agar message fetch nahi hua (ya caption nahi tha), toh skip karo
                    if msg_id not in messages_dict:
                        skipped_count += 1
                        continue
                        
                    original_caption = messages_dict[msg_id].caption
                    
                    try:
                        final_caption = original_caption
                        needs_edit = False
                        
                        # Step 1: Clean karo (agar mode 'clean' ya 'replace' hai)
                        if edit_mode == "clean" or edit_mode == "replace":
                            if locked_content:
                                # Locked content ko bachakar clean karo
                                placeholder = "___LOCKED_CONTENT_PLACEHOLDER___"
                                temp_caption = original_caption.replace(locked_content, placeholder)
                                cleaned_temp_caption = LINK_USERNAME_REGEX.sub("", temp_caption)
                                final_caption = cleaned_temp_caption.replace(placeholder, locked_content).strip()
                            else:
                                # Normal clean
                                final_caption = LINK_USERNAME_REGEX.sub("", original_caption).strip()
                        
                        # Step 2: Add karo (agar mode 'add' ya 'replace' hai)
                        if edit_mode == "add" or edit_mode == "replace":
                            if locked_content and locked_content not in final_caption:
                                final_caption += f"\n\n{locked_content}"
                                final_caption = final_caption.strip()
                        
                        # Check karo ki kya caption sach me badla hai
                        if final_caption != original_caption:
                            needs_edit = True
                        
                        # Check karo agar caption poora clean ho gaya
                        if not final_caption and original_caption:
                            needs_edit = True
                        
                        if needs_edit:
                            # Agar edit zaroori hai, toh task list me daalo
                            tasks_to_run.append(edit_caption_task(chat_id, msg_id, final_caption))
                        else:
                            skipped_count += 1
                            
                    except Exception as e:
                        print(f"[EDIT LOGIC ERR] Msg {msg_id}: {e}")
                        failed_count += 1
                        if first_error_message is None: first_error_message = str(e)

                if not GLOBAL_TASK_RUNNING: break
                
                # Ab saare tasks ko ek saath (concurrently) chalao
                if tasks_to_run:
                    print(f"Running batch {i+1} with {len(tasks_to_run)} edit tasks concurrently...")
                    results = await asyncio.gather(*tasks_to_run, return_exceptions=True)
                    
                    # Results check karo
                    for res in results:
                        if res == "success":
                            edited_count += 1
                        elif res == "skipped":
                            skipped_count += 1
                        elif isinstance(res, Exception):
                            failed_count += 1
                            if first_error_message is None:
                                first_error_message = str(res)
                
                processed_count += batch_processed_count

                if not GLOBAL_TASK_RUNNING: break

                try:
                    await status.edit(
                        f"⏳ Processing... (Mode: `{edit_mode.upper()}`)\n"
                        f"Batch {i+1}/{total_batches} complete.\n\n"
                        f"Total Processed: {processed_count}/{total_messages}\n"
                        f"✅ Edited: {edited_count}\n"
                        f"ℹ️ Skipped (No Change): {skipped_count}\n"
                        f"❌ Failed: {failed_count}",
                        reply_markup=STOP_BUTTON
                    )
                except FloodWait: pass
                
                await asyncio.sleep(2) # Thoda rest do batches ke beech

        except ChatAdminRequired:
            await status.edit(f"❌ **Error: Main Admin nahi hoon!**\nMujhe channel `{chat_name}` (ID: `{chat_id}`) mein 'Edit Messages' permission ke saath Admin banao.", reply_markup=None)
            return
        except Exception as e:
            await status.edit(f"❌ Ek bada error aaya: {e}", reply_markup=None)
            return
        
        final_message_text = f"🎉 **Caption Editing Complete!** (Mode: `{edit_mode.upper()}`)\n"
        if not GLOBAL_TASK_RUNNING:
            final_message_text = f"🛑 **Caption Editing Stopped!** (Mode: `{edit_mode.upper()}`)\n"

        final_message_text += f"Channel: `{chat_name}`\n\n" \
                              f"Total Processed: {processed_count}/{total_messages}\n" \
                              f"✅ Total Edited: {edited_count}\n" \
                              f"ℹ️ Total Skipped (No Change): {skipped_count}\n" \
                              f"❌ Total Failed: {failed_count}"

        if failed_count > 0 and first_error_message:
            final_message_text += f"\n\n**Pehla Error:** `{first_error_message}`"
            if "MESSAGE_ID_INVALID" in first_error_message or "CHAT_ADMIN_REQUIRED" in first_error_message:
                final_message_text += f"\n**Tippani:** 'MESSAGE_ID_INVALID' ya 'CHAT_ADMIN_REQUIRED' ka matlab hai ki bot channel (ID: `{chat_id}`) me admin nahi hai, ya uske paas 'Edit messages' ki permission nahi hai."
        
        await status.edit(final_message_text, reply_markup=None)

    finally:
        GLOBAL_TASK_RUNNING = False

@app.on_message(filters.command("clean_captions") & filters.create(only_admin))
async def clean_captions_cmd(_, message):
    app.loop.create_task(batch_edit_captions(message, edit_mode="clean"))

@app.on_message(filters.command("add_locked_content") & filters.create(only_admin))
async def add_locked_content_cmd(_, message):
    app.loop.create_task(batch_edit_captions(message, edit_mode="add"))

@app.on_message(filters.command("replace_all_content") & filters.create(only_admin))
async def replace_all_content_cmd(_, message):
    app.loop.create_task(batch_edit_captions(message, edit_mode="replace"))

# --- Status & Ping ---
@app.on_message(filters.command("status") & filters.create(only_admin))
async def status_cmd(_, message):
    # Movie Stats
    total_in_movie_index = len(movie_index.get('movies', {}))
    total_in_target_movie_ids = 0
    total_in_target_movie_comp_keys = 0
    if os.path.exists(TARGET_MOVIE_INDEX_DB_FILE):
        try:
            with open(TARGET_MOVIE_INDEX_DB_FILE, "r") as f:
                data = json.load(f)
                total_in_target_movie_ids = len(data.get("unique_ids", []))
                total_in_target_movie_comp_keys = len(data.get("compound_keys", []))
        except: pass 
    
    # Webseries Stats
    total_in_webseries_index = 0
    if os.path.exists(WEBSERIES_INDEX_DB_FILE):
        try:
            with open(WEBSERIES_INDEX_DB_FILE, "r") as f:
                total_in_webseries_index = len(json.load(f))
        except: pass

    total_in_target_ws_ids = 0
    total_in_target_ws_comp_keys = 0
    if os.path.exists(TARGET_WEBSERIES_INDEX_DB_FILE):
        try:
            with open(TARGET_WEBSERIES_INDEX_DB_FILE, "r") as f:
                data = json.load(f)
                total_in_target_ws_ids = len(data.get("unique_ids", []))
                total_in_target_ws_comp_keys = len(data.get("compound_keys", []))
        except: pass 

    # NAYA: Full Forward Stats
    total_in_full_index = 0
    if os.path.exists(FULL_SOURCE_INDEX_DB_FILE):
        try:
            with open(FULL_SOURCE_INDEX_DB_FILE, "r") as f:
                total_in_full_index = len(json.load(f))
        except: pass

    total_in_target_full_ids = 0
    total_in_target_full_comp_keys = 0
    if os.path.exists(FULL_TARGET_INDEX_DB_FILE):
        try:
            with open(FULL_TARGET_INDEX_DB_FILE, "r") as f:
                data = json.load(f)
                total_in_target_full_ids = len(data.get("unique_ids", []))
                total_in_target_full_comp_keys = len(data.get("compound_keys", []))
        except: pass

    # General Stats
    total_in_fwd_db = 0
    if os.path.exists(DUPLICATE_DB_FILE):
        try:
            with open(DUPLICATE_DB_FILE, "r") as f:
                total_in_fwd_db = len(f.readlines())
        except: pass
    
    # Editing Stats
    total_in_editing_index = 0
    editing_index_chat = "N/A"
    if os.path.exists(EDITING_INDEX_DB_FILE):
        try:
            with open(EDITING_INDEX_DB_FILE, "r") as f:
                data = json.load(f)
                total_in_editing_index = len(data.get("message_ids", []))
                editing_index_chat = data.get("chat_name", "N/A")
        except: pass
            
    await message.reply(
        f"📊 **Bot Status**\n\n"
        f"--- **Session** ---\n"
        f"**Task Running:** `{GLOBAL_TASK_RUNNING}`\n"
        f"Forwarded (Last Run): `{forwarded_count}`\n"
        f"Mode: `{'COPY' if mode_copy else 'FORWARD'}`\n"
        f"Bot-Forwarded DB: `{total_in_fwd_db}` entries\n"
        f"--- **🚀 Full Forward** ---\n"
        f"Indexed Source (All): `{total_in_full_index}`\n"
        f"Indexed Target (All IDs): `{total_in_target_full_ids}`\n"
        f"Indexed Target (All Name+Size): `{total_in_target_full_comp_keys}`\n"
        f"--- **🎬 Movies (Alag)** ---\n"
        f"Movie Target: `{target_channel}`\n"
        f"Movie Limit: `{limit_messages}`\n"
        f"Indexed Source Movies: `{total_in_movie_index}`\n"
        f"Indexed Target (IDs): `{total_in_target_movie_ids}`\n"
        f"Indexed Target (Name+Size): `{total_in_target_movie_comp_keys}`\n"
        f"--- **📺 Web Series (Alag)** ---\n"
        f"Indexed Source Episodes: `{total_in_webseries_index}`\n"
        f"Indexed Target (IDs): `{total_in_target_ws_ids}`\n"
        f"Indexed Target (Name+Size): `{total_in_target_ws_comp_keys}`\n"
        f"--- **✒️ Editing** ---\n"
        f"Locked Content: `{locked_content[:50] if locked_content else 'None'}`\n"
        f"Editing Index Channel: `{editing_index_chat}`\n"
        f"Editing Index (Messages): `{total_in_editing_index}`"
    )

@app.on_message(filters.command("sync") & filters.create(only_admin))
async def sync_chats(_, message):
    if GLOBAL_TASK_RUNNING:
        await message.reply("❌ Bot busy hai, abhi sync nahi kar sakta.")
        return
    
    async def runner():
        status = await message.reply("⏳ Syncing User Account chats...")
        count = 0
        skipped = 0
        
        try:
            dialog_iterator = app.get_dialogs()
            
            # Python 3.10+ में anext() का उपयोग async iterator के लिए
            while True:
                try:
                    dialog = await anext(dialog_iterator)
                    
                    # यह सुनिश्चित करने के लिए कि dialog object ठीक से लोड हो
                    _ = dialog.chat.id 
                    count += 1
                    
                except StopAsyncIteration:
                    break 
                    
                except (PeerIdInvalid, RPCError, ChannelPrivate) as e:
                    # RPCError (जैसे CHANNEL_PRIVATE, USERNAME_INVALID) को पकड़कर skip करें
                    print(f"❌ [SYNC ERR] Skipping dialog due to error: {e}")
                    skipped += 1
                    continue
                    
                except Exception as e:
                    # कोई अन्य अप्रत्याशित त्रुटि
                    print(f"❌ [SYNC FATAL ERR] Unknown error processing dialog: {e}")
                    skipped += 1
                    continue
                    
            await status.edit(f"✅ User Account Cache synced! Found {count} chats. (Skipped Errors: {skipped})")
            
        except Exception as e:
            # यह बाहरी ब्लॉक तब चलेगा जब get_dialogs() iterator ही क्रैश हो जाए
            await status.edit(f"❌ Sync failed: {e}")
            
    app.loop.create_task(runner())


@app.on_message(filters.command("ping") & filters.create(only_admin))
async def ping(_, message):
    await message.reply("✅ Alive | Polling | Ready")

# --- Auto-Restart Loop ---
print("Loading databases (Full Forward Mode)...")
load_movie_duplicate_dbs()
load_webseries_duplicate_dbs()
load_full_duplicate_dbs() # NAYA
load_movie_index_db()
load_locked_content()
print(f"Loaded {len(movie_fwd_unique_ids)} movie unique IDs.")
print(f"Loaded {len(webseries_fwd_unique_ids)} webseries unique IDs.")
print(f"Loaded {len(full_fwd_unique_ids)} full forward unique IDs.")
print(f"Loaded {len(movie_index.get('movies', {}))} indexed movies from {movie_index}")
print("✅ UserBot ready — send commands.")

while True:
    try:
        print("Bot ko start kar raha hoon...")
        app.run()
        print("Bot ruk गया है. 10 second me restart kar raha hoon...")
    
    except RPCError as rpc_e:
        print(f"❌ [CRITICAL RPCError] Pyrogram client crash hua: {rpc_e}")
        print("Restarting in 60 seconds...")
        time.sleep(60)
        
    except Exception as e:
        print(f"❌ [CRITICAL ERROR] Bot loop crash hua: {e}")
        print("Restarting in 10 seconds...")
    
    print("Restarting loop...")
    time.sleep(10)
