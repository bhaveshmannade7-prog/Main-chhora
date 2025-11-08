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
PER_MSG_DELAY = 0.5 
BATCH_SIZE_FOR_BREAK = 250
BREAK_DURATION_SEC = 25 

# --- Database Files ---
DUPLICATE_DB_FILE = "forwarded_unique_ids.txt"
# Movie Databases
MOVIE_INDEX_DB_FILE = "movie_database.json"
TARGET_MOVIE_INDEX_DB_FILE = "target_movie_index.json" # Renamed for clarity
# Webseries Databases (Naya)
WEBSERIES_INDEX_DB_FILE = "webseries_database.json"
TARGET_WEBSERIES_INDEX_DB_FILE = "target_webseries_index.json"
# Utility Databases
BAD_QUALITY_DB_FILE = "bad_quality_movies.json" 

# In-memory sets
movie_fwd_unique_ids = set()
movie_target_compound_keys = set() 
webseries_fwd_unique_ids = set()
webseries_target_compound_keys = set() 

# Bad Quality Keywords
BAD_QUALITY_KEYWORDS = [
    r"cam", r"camrip", r"hdcam", r"ts", r"telesync", r"tc", 
    r"\(line\)", r"\(clean\)", r"line audio", r"bad audio",
    r"screen record", r"screener", r"hq-cam"
]
BAD_QUALITY_REGEX = re.compile(r'\b(?:' + '|'.join(BAD_QUALITY_KEYWORDS) + r')\b', re.IGNORECASE)

# --- Web Series Regex (The "Brain") ---
# S01 E01, S01E01, Season 01 Episode 01, S1-EP1
EPISODE_REGEX = re.compile(
    r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(?:E|Ep|Episode)\s*(\d{1,3})(?!.*\d)",
    re.IGNORECASE | re.DOTALL
)
# S01 E01-E04, S01E01-04
EPISODE_PACK_REGEX = re.compile(
    r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(?:E|Ep|Episode)\s*(\d{1,3})\s*-\s*(\d{1,3})",
    re.IGNORECASE | re.DOTALL
)
# Season 01 Complete
SEASON_COMPLETE_REGEX = re.compile(
    r"(.*?)(?:S|Season)\s*(\d{1,2})\s*(Complete)",
    re.IGNORECASE | re.DOTALL
)
# Episode 01 (Jaise Ishq Murshid)
EPISODE_ONLY_REGEX = re.compile(
    r"(.*?)(?:Episode|Ep)\s*(\d{1,3})(?!.*\d)",
    re.IGNORECASE | re.DOTALL
)
# S01, S02... (Agar file name mein ho)
SIMPLE_SEASON_REGEX = re.compile(
    r"\b(S\d{1,2})\b", re.IGNORECASE
)
# Keywords to detect if it's a series
SERIES_KEYWORDS_REGEX = re.compile(
    r"S\d{1,2}|Season|\bEp\b|Episode", re.IGNORECASE
)
# ------------------------------------

def get_default_movie_index():
    return {
        "source_channel_id": None,
        "source_channel_name": None,
        "movies": {}
    }
movie_index = get_default_movie_index()


# --- Database Load/Save Functions ---

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
                
                target_unique_ids = target_data.get("unique_ids", [])
                movie_fwd_unique_ids.update(target_unique_ids)
                print(f"Loaded {len(target_unique_ids)} movie unique_ids from target index.")
                
                target_comp_keys = target_data.get("compound_keys", [])
                movie_target_compound_keys.update(target_comp_keys)
                print(f"Loaded {len(target_comp_keys)} movie compound_keys from target index.")
                
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
                
                target_unique_ids = target_data.get("unique_ids", [])
                webseries_fwd_unique_ids.update(target_unique_ids)
                print(f"Loaded {len(target_unique_ids)} webseries unique_ids from target index.")
                
                target_comp_keys = target_data.get("compound_keys", [])
                webseries_target_compound_keys.update(target_comp_keys)
                print(f"Loaded {len(target_comp_keys)} webseries compound_keys from target index.")
                
        except Exception as e:
            print(f"[DB ERR] loading target webseries index DB: {e}")

def save_forwarded_id(unique_id, compound_key, db_type="movie"):
    try:
        with open(DUPLICATE_DB_FILE, "a") as f:
            f.write(f"{unique_id}\n")
        
        if db_type == "movie":
            movie_fwd_unique_ids.add(unique_id)
            if compound_key:
                movie_target_compound_keys.add(compound_key)
        else: # webseries
            webseries_fwd_unique_ids.add(unique_id)
            if compound_key:
                webseries_target_compound_keys.add(compound_key)
            
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
        raise RuntimeError("❌ Peer not known. **Run /sync first!**")
    except RPCError as e:
        raise RuntimeError(f"❌ Resolve failed: {e}")

# --- /start command (Updated) ---
START_MESSAGE = """
**🚀 Welcome, Admin! (Movie & Web Series Bot)**

**Naya Feature:** Bot ab Movies aur Web Series ko alag-alag handle kar sakta hai aur unhe sort bhi kar sakta hai.

--- **🎬 MOVIE COMMANDS** ---
* `/index <chat_id>` - Source ko scan karke `movie_database.json` banata hai.
* `/index_target <chat_id>` - Target ko scan karke `target_movie_index.json` banata hai.
* `/clear_index` - Source movie index (`.json`) delete karta hai.
* `/clear_target_index` - Target movie index (`.json`) delete karta hai.
* `/start_forward` - Movies ki forwarding shuru karta hai.

--- **📺 WEB SERIES COMMANDS** ---
* `/index_webseries <chat_id>` - **(Naya)** Source ko scan karke *sorted* `webseries_database.json` banata hai.
* `/index_target_webseries <chat_id>` - **(Naya)** Target ko scan karke `target_webseries_index.json` banata hai.
* `/clear_webseries_index` - **(Naya)** Source webseries index (`.json`) delete karta hai.
* `/clear_target_webseries_index` - **(Naya)** Target webseries index (`.json`) delete karta hai.
* `/forward_webseries <target_chat_id> [limit]` - **(Naya)** Sorted webseries ko forward karta hai.

--- **🛠️ UTILITY COMMANDS** ---
* `/set_target <chat_id>` - (Movies ke liye) Target channel set karein.
* `/set_limit <number>` - (Movies ke liye) Max limit.
* `/mode <copy/forward>` - `copy` (default) ya `forward`.
* `/status` - Current status dikhata hai.
* `/sync` - Bot ke cache ko sync karta hai.
* `/ping` - Bot zinda hai ya nahi.
* `/start` - Yeh help message.

--- **⚠️ DANGER ZONE / UTILITY** ---
* `/clean_dupes <chat_id>` - Channel se duplicate movies ko scan karke delete karta hai. (ADMIN Permission Chahiye)
* `/find_bad_quality <chat_id>` - Low-quality audio (TC, CAM) movies ko `bad_quality_movies.json` mein save karta hai.
* `/forward_bad_quality <target_chat_id> [limit]` - `bad_quality_movies.json` se movies ko forward karta hai.
"""

@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_cmd(_, message):
    await message.reply(START_MESSAGE, disable_web_page_preview=True)
# ---------------------------

def get_media_details(m):
    media = m.video or m.document
    if not media:
        return None, None, None
        
    file_name = getattr(media, 'file_name', None)
    file_size = getattr(media, 'file_size', None)
    file_unique_id = getattr(media, 'file_unique_id', None)
    
    return file_name, file_size, file_unique_id

# --- /index (Movies) ---
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
                processed_stage1 += 1
                try:
                    file_name, file_size, unique_id = get_media_details(m)
                    if not unique_id: continue 
                    
                    text_to_check = (file_name or "") + " " + (m.caption or "")
                    if SERIES_KEYWORDS_REGEX.search(text_to_check):
                        continue # Yeh web series hai, skip karo
                    
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

            await status.edit(f"⏳ Indexing Movies... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} unique")

            # Stage 2: Documents (Files)
            async for m in app.search_messages(src_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                    
                    file_name, file_size, unique_id = get_media_details(m)
                    if not unique_id: continue

                    text_to_check = (file_name or "") + " " + (m.caption or "")
                    if SERIES_KEYWORDS_REGEX.search(text_to_check):
                        continue # Yeh web series hai, skip karo

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

            save_movie_index_db()
            await status.edit(f"🎉 Movie Indexing Complete!\nChannel: `{src_name}`\nFound: **{found_count}** unique movies.\n\nDatabase ko `movie_database.json` me save kar diya hai.")

        except Exception as e:
            await status.edit(f"❌ Movie Indexing Error: `{e}`")

    app.loop.create_task(runner())
# ---------------------------------

# --- /index_target (Movies) ---
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
        
        target_unique_ids = set()
        target_compound_keys_set = set()
        
        status = await message.reply(f"⏳ Target Movie Indexing shuru ho raha hai: `{tgt_name}`...\n(Stage 1: Videos)")

        processed_stage1 = 0
        processed_stage2 = 0
        
        try:
            # Stage 1: Videos
            async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                processed_stage1 += 1
                try:
                    text_to_check = (getattr(m.video, 'file_name', "") or "") + " " + (m.caption or "")
                    if SERIES_KEYWORDS_REGEX.search(text_to_check):
                        continue # Web Series, skip
                        
                    file_name, file_size, unique_id = get_media_details(m)
                    if unique_id:
                        target_unique_ids.add(unique_id)
                    if file_name and file_size:
                        target_compound_keys_set.add(f"{file_name}-{file_size}")
                except Exception as e: print(f"[INDEX_TGT S1 ERR] Msg {m.id}: {e}")
                
                if processed_stage1 % 500 == 0:
                    try: await status.edit(f"⏳ Indexing Target Movies... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")
                    except FloodWait: pass 

            await status.edit(f"⏳ Indexing Target Movies... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")

            # Stage 2: Documents (Files)
            async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1  # <-- Yahaan galti thi (processed_s2 -> processed_stage2)
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                    
                    text_to_check = (getattr(m.document, 'file_name', "") or "") + " " + (m.caption or "")
                    if SERIES_KEYWORDS_REGEX.search(text_to_check):
                        continue # Web Series, skip

                    file_name, file_size, unique_id = get_media_details(m)
                    if unique_id:
                        target_unique_ids.add(unique_id)
                    if file_name and file_size:
                        target_compound_keys_set.add(f"{file_name}-{file_size}")
                except Exception as e: print(f"[INDEX_TGT S2 ERR] Msg {m.id}: {e}")

                if processed_stage2 % 500 == 0: # <-- Yahaan galti thi (processed_s2 -> processed_stage2)
                    try: await status.edit(f"⏳ Indexing Target Movies... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(target_unique_ids)} unique")
                    except FloodWait: pass
            
            target_db_data = {
                "unique_ids": list(target_unique_ids),
                "compound_keys": list(target_compound_keys_set)
            }
            with open(TARGET_MOVIE_INDEX_DB_FILE, "w") as f:
                json.dump(target_db_data, f)
            
            load_movie_duplicate_dbs() 
            
            await status.edit(f"🎉 Target Movie Indexing Complete!\nChannel: `{tgt_name}`\nFound: **{len(target_unique_ids)}** unique IDs.\nFound: **{len(target_compound_keys_set)}** compound keys (name+size).\n\nDuplicate list update ho gayi hai.")

        except Exception as e:
            await status.edit(f"❌ Target Movie Indexing Error: `{e}`")

    app.loop.create_task(runner())
# ------------------------------------

# --- NAYA COMMAND: /index_webseries ---
def parse_series_info(text):
    text = text.replace('\n', ' ').replace('.', ' ') # Clean text for regex
    
    # Check S01 E01-04
    match = EPISODE_PACK_REGEX.search(text)
    if match:
        name = match.group(1).strip()
        season = int(match.group(2))
        ep_start = int(match.group(3))
        ep_end = int(match.group(4))
        return name, season, ep_start, ep_end

    # Check S01 E01
    match = EPISODE_REGEX.search(text)
    if match:
        name = match.group(1).strip()
        season = int(match.group(2))
        ep_start = int(match.group(3))
        return name, season, ep_start, None

    # Check Season 01 Complete
    match = SEASON_COMPLETE_REGEX.search(text)
    if match:
        name = match.group(1).strip()
        season = int(match.group(2))
        return name, season, 999, None # 999 to sort complete last

    # Check Episode 01 (assume S01)
    match = EPISODE_ONLY_REGEX.search(text)
    if match:
        name = match.group(1).strip()
        season = 1 # Assume Season 1
        ep_start = int(match.group(2))
        return name, season, ep_start, None
        
    return None, 0, 0, 0 # Not a series

@app.on_message(filters.command("index_webseries") & filters.create(only_admin))
async def index_webseries_cmd(_, message):
    try:
        source_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_webseries -100123...` or `/index_webseries @channel`")
        return

    async def runner():
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
            # Stage 1: Videos
            async for m in app.search_messages(src_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                processed_stage1 += 1
                try:
                    file_name, file_size, unique_id = get_media_details(m)
                    if not unique_id: continue 
                    
                    text_to_check = (file_name or "") + " " + (m.caption or "")
                    if not SERIES_KEYWORDS_REGEX.search(text_to_check):
                        continue # Movie hai, skip karo
                    
                    series_name, season_num, ep_start, ep_end = parse_series_info(text_to_check)
                    if not series_name:
                        # Simple S01 check
                        if SIMPLE_SEASON_REGEX.search(text_to_check):
                             series_name = file_name or "Unknown Series"
                             season_num = 1
                             ep_start = 1
                        else:
                             continue # Keyword tha par parse nahi hua
                    
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
                except Exception as e: print(f"[INDEX_WS S1 ERR] Msg {m.id}: {e}")
                
                if processed_stage1 % 500 == 0:
                    try: await status.edit(f"⏳ Indexing Web Series... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {found_count} episodes")
                    except FloodWait: pass 

            await status.edit(f"⏳ Indexing Web Series... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {found_count} episodes")

            # Stage 2: Documents (Files)
            async for m in app.search_messages(src_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                    
                    file_name, file_size, unique_id = get_media_details(m)
                    if not unique_id: continue

                    text_to_check = (file_name or "") + " " + (m.caption or "")
                    if not SERIES_KEYWORDS_REGEX.search(text_to_check):
                        continue # Movie hai, skip karo

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
            
            await status.edit(f"⏳ Scan complete. Found {found_count} episodes.\nAb sorting shuru kar raha hoon (line se jama raha hoon)...")
            
            # --- YEH HAI SORTING KA JAADU ---
            sorted_webseries_list = sorted(
                temp_webseries_list, 
                key=lambda x: (
                    x['series_name'].lower().strip(), # 1. Series ke naam se
                    x['season_num'],                  # 2. Season number se
                    x['episode_num']                  # 3. Episode number se
                )
            )
            # ------------------------------------
            
            try:
                with open(WEBSERIES_INDEX_DB_FILE, "w", encoding="utf-8") as f:
                    json.dump(sorted_webseries_list, f, indent=2, ensure_ascii=False)
            except Exception as e:
                await status.edit(f"❌ Database save nahi kar paaya: {e}")
                return

            await status.edit(f"🎉 Web Series Indexing & Sorting Complete!\nChannel: `{src_name}`\nFound: **{found_count}** total episodes.\n\nSorted database ko `webseries_database.json` me save kar diya hai.")

        except Exception as e:
            await status.edit(f"❌ Web Series Indexing Error: `{e}`")

    app.loop.create_task(runner())
# ------------------------------------

# --- NAYA COMMAND: /index_target_webseries ---
@app.on_message(filters.command("index_target_webseries") & filters.create(only_admin))
async def index_target_webseries_cmd(_, message):
    try:
        target_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/index_target_webseries -100123...` or `/index_target_webseries @channel`")
        return

    async def runner():
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
                processed_stage1 += 1
                try:
                    text_to_check = (getattr(m.video, 'file_name', "") or "") + " " + (m.caption or "")
                    if not SERIES_KEYWORDS_REGEX.search(text_to_check):
                        continue # Movie hai, skip
                        
                    file_name, file_size, unique_id = get_media_details(m)
                    if unique_id:
                        target_unique_ids.add(unique_id)
                    if file_name and file_size:
                        target_compound_keys_set.add(f"{file_name}-{file_size}")
                except Exception as e: print(f"[INDEX_TGT_WS S1 ERR] Msg {m.id}: {e}")
                
                if processed_stage1 % 500 == 0:
                    try: await status.edit(f"⏳ Indexing Target Web Series... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")
                    except FloodWait: pass 

            await status.edit(f"⏳ Indexing Target Web Series... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(target_unique_ids)} unique")

            # Stage 2: Documents (Files)
            async for m in app.search_messages(tgt_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1 # <-- Yahaan galti thi (processed_s2 -> processed_stage2)
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                    
                    text_to_check = (getattr(m.document, 'file_name', "") or "") + " " + (m.caption or "")
                    if not SERIES_KEYWORDS_REGEX.search(text_to_check):
                        continue # Movie hai, skip

                    file_name, file_size, unique_id = get_media_details(m)
                    if unique_id:
                        target_unique_ids.add(unique_id)
                    if file_name and file_size:
                        target_compound_keys_set.add(f"{file_name}-{file_size}")
                except Exception as e: print(f"[INDEX_TGT_WS S2 ERR] Msg {m.id}: {e}")

                if processed_stage2 % 500 == 0: # <-- Yahaan galti thi (processed_s2 -> processed_stage2)
                    try: await status.edit(f"⏳ Indexing Target Web Series... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(target_unique_ids)} unique")
                    except FloodWait: pass
            
            target_db_data = {
                "unique_ids": list(target_unique_ids),
                "compound_keys": list(target_compound_keys_set)
            }
            with open(TARGET_WEBSERIES_INDEX_DB_FILE, "w") as f:
                json.dump(target_db_data, f)
            
            load_webseries_duplicate_dbs() 
            
            await status.edit(f"🎉 Target Web Series Indexing Complete!\nChannel: `{tgt_name}`\nFound: **{len(target_unique_ids)}** unique IDs.\nFound: **{len(target_compound_keys_set)}** compound keys.\n\nDuplicate list update ho gayi hai.")

        except Exception as e:
            await status.edit(f"❌ Target Web Series Indexing Error: `{e}`")

    app.loop.create_task(runner())
# ------------------------------------

# --- NAYA COMMAND: /clean_dupes (Duplicate Cleaner) ---
@app.on_message(filters.command("clean_dupes") & filters.create(only_admin))
async def clean_dupes_cmd(_, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/clean_dupes -100123...` or `/clean_dupes @channel`\n\n**Warning:** Aapka user account channel mein ADMIN (Delete Permission) hona chahiye.")
        return

    async def runner():
        try:
            chat = await resolve_chat_id(app, chat_ref)
            chat_id = chat.id
            chat_name = chat.title or chat.username
        except Exception as e:
            await message.reply(str(e))
            return
        
        status = await message.reply(f"⏳ **Duplicate Cleaner**\nScanning: `{chat_name}`...\n(Yeh process bohot time le sakta hai!)\n\n(Stage 1: Videos)")
        
        seen_movies = {} # Key: "name-size", Value: message_id
        messages_to_delete = []
        processed_stage1 = 0
        processed_stage2 = 0

        try:
            # Stage 1: Videos
            async for m in app.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO, limit=0):
                processed_stage1 += 1
                try:
                    file_name, file_size, unique_id = get_media_details(m)
                    if not file_name or not file_size: continue
                    
                    compound_key = f"{file_name}-{file_size}"
                    
                    if compound_key in seen_movies:
                        messages_to_delete.append(m.id) # Found a dupe
                    else:
                        seen_movies[compound_key] = m.id # First time
                except Exception as e: print(f"[CLEAN S1 ERR] Msg {m.id}: {e}")
                
                if processed_stage1 % 500 == 0:
                    try: await status.edit(f"⏳ Scanning... (Stage 1)\nProcessed: {processed_stage1} videos\nFound: {len(messages_to_delete)} duplicates")
                    except FloodWait: pass

            # Stage 2: Documents
            await status.edit(f"⏳ Scanning... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(messages_to_delete)} duplicates")
            
            async for m in app.search_messages(chat_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1 # <-- Yahaan galti thi (processed_s2 -> processed_stage2)
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                    file_name, file_size, unique_id = get_media_details(m)
                    if not file_name or not file_size: continue
                    
                    compound_key = f"{file_name}-{file_size}"
                    
                    if compound_key in seen_movies:
                        messages_to_delete.append(m.id)
                    else:
                        seen_movies[compound_key] = m.id
                except Exception as e: print(f"[CLEAN S2 ERR] Msg {m.id}: {e}")

                if processed_stage2 % 500 == 0: # <-- Yahaan galti thi (processed_s2 -> processed_stage2)
                    try: await status.edit(f"⏳ Scanning... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(messages_to_delete)} duplicates")
                    except FloodWait: pass

            # Deletion Phase
            total_to_delete = len(messages_to_delete)
            if total_to_delete == 0:
                await status.edit("🎉 Scan complete. Koi 'Name+Size' duplicate nahi mila!")
                return

            await status.edit(f"✅ Scan complete.\nFound **{total_to_delete}** duplicates.\nAb 100 ke batch me delete karna shuru kar raha hoon...")
            
            deleted_count = 0
            batches = [messages_to_delete[i:i + 100] for i in range(0, total_to_delete, 100)]
            
            for i, batch in enumerate(batches):
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

            await status.edit(f"🎉 Cleanup Complete!\nDeleted {deleted_count} duplicate movies from `{chat_name}`.")
        
        except ChatAdminRequired:
            await status.edit("❌ **Error: Main Admin nahi hoon!**\nMujhe channel/group mein 'Delete Messages' permission ke saath Admin banao.")
        except Exception as e:
            await status.edit(f"❌ Error during scan/delete process: {e}")

    app.loop.create_task(runner())
# ------------------------------------

# --- NAYA COMMAND: /find_bad_quality ---
@app.on_message(filters.command("find_bad_quality") & filters.create(only_admin))
async def find_bad_quality_cmd(_, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
    except:
        await message.reply("❌ Usage:\n`/find_bad_quality -100123...` or `/find_bad_quality @channel`")
        return

    async def runner():
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

            await status.edit(f"⏳ Scanning... (Stage 2: Files)\nProcessed: {processed_stage1} videos\nFound: {len(bad_quality_movies_list)} bad quality movies")
            
            # Stage 2: Documents
            async for m in app.search_messages(chat_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
                processed_stage2 += 1 # <-- Yahaan galti thi (processed_s2 -> processed_stage2)
                try:
                    if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                    
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

                if processed_stage2 % 500 == 0: # <-- Yahaan galti thi (processed_s2 -> processed_stage2)
                    try: await status.edit(f"⏳ Scanning... (Stage 2)\nProcessed: {processed_stage2} files\nFound: {len(bad_quality_movies_list)} bad quality movies")
                    except FloodWait: pass
            
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
            await status.edit(f"❌ Error during bad quality scan: {e}")

    app.loop.create_task(runner())
# ------------------------------------

# --- NAYA COMMAND: /forward_bad_quality ---
@app.on_message(filters.command("forward_bad_quality") & filters.create(only_admin))
async def forward_bad_quality_cmd(_, message):
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

    async def runner():
        try:
            target_chat = await resolve_chat_id(app, target_ref)
            tgt_id = target_chat.id
            tgt_name = target_chat.title or target_chat.username
        except Exception as e:
            await message.reply(str(e))
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
            f"Progress: 0/{total_to_forward}"
        )
        
        forwarded_count = 0
        batch_counter = 0

        for movie in bad_quality_movies_data:
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
                        await status.edit(f"⏳ Forwarding bad quality movies...\nProgress: {forwarded_count}/{total_to_forward}\nTaking a short break (5s)...")
                        await asyncio.sleep(5) 
                        await status.edit(f"⏳ Forwarding bad quality movies...\nProgress: {forwarded_count}/{total_to_forward}")
                    except FloodWait: pass 

            except FloodWait as e:
                await status.edit(f"⏳ FloodWait: sleeping {e.value}s…")
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
                    await status.edit(f"⏳ Forwarding bad quality movies...\nProgress: {forwarded_count}/{total_to_forward}")
            except FloodWait: pass


        await status.edit(
            f"🎉 **Bad Quality Movies Forwarding Complete!**\n"
            f"Total Forwarded: `{forwarded_count}` to `{tgt_name}`."
        )

    app.loop.create_task(runner())
# ------------------------------------

# --- Clear Commands (Updated) ---
@app.on_message(filters.command("clear_index") & filters.create(only_admin))
async def clear_index_cmd(_, message):
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
# ------------------------------------

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
            await message.reply("⚠ (Movie) Pehle `/set_target` set karo.")
            return
            
        if not movie_index["source_channel_id"] or not movie_index["movies"]:
            await message.reply("⚠ Movie index khaali hai. Pehle `/index <channel_id>` chalao.")
            return

        try:
            tgt_chat = await resolve_chat_id(app, target_channel)
            tgt = tgt_chat.id
            src = movie_index["source_channel_id"]
            src_name = movie_index["source_channel_name"]
        except Exception as e:
            await message.reply(str(e))
            return
        
        load_movie_duplicate_dbs()

        is_forwarding = True
        forwarded_count = 0
        duplicate_count = 0
        processed_count = 0
        total_to_forward = len(movie_index["movies"])

        status = await message.reply(
            f"⏳ **Movie Forwarding** shuru ho raha hai...\n"
            f"Source (Cache): `{src_name}`\n"
            f"Target: `{tgt_chat.title or tgt_chat.username}`\n"
            f"Total Movies in Index: `{total_to_forward}`\n"
            f"Total Duplicates (Loaded): `{len(movie_fwd_unique_ids)}` (IDs) + `{len(movie_target_compound_keys)}` (Name+Size)",
            reply_markup=STOP_BUTTON
        )
        
        movies_list = list(movie_index["movies"].items())
        
        try:
            for unique_id, data in movies_list:
                if not is_forwarding: break
                
                processed_count += 1
                message_id = data["message_id"]
                file_name = data.get("file_name")
                file_size = data.get("file_size")
                compound_key = None
                
                if file_name and file_size is not None:
                    compound_key = f"{file_name}-{file_size}"

                try:
                    if unique_id in movie_fwd_unique_ids:
                        duplicate_count += 1
                        continue
                        
                    if compound_key and compound_key in movie_target_compound_keys:
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
                            f"✅ Fwd: `{forwarded_count}` / {(limit_messages or '∞')}, 🔍 Dup: `{duplicate_count}`\n"
                            f"⏳ Processed: {processed_count} / {total_to_forward}",
                            reply_markup=STOP_BUTTON
                        )
                    except FloodWait: pass 

                if forwarded_count > 0 and forwarded_count % BATCH_SIZE_FOR_BREAK == 0 and is_forwarding:
                    try:
                        await status.edit_text(
                            f"✅ Fwd: `{forwarded_count}`. 5 batch complete.\n"
                            f"☕ {BREAK_DURATION_SEC} second ka break le raha hoon...",
                            reply_markup=STOP_BUTTON
                        )
                    except FloodWait: pass
                    
                    await asyncio.sleep(BREAK_DURATION_SEC) 
                
                if limit_messages and forwarded_count >= limit_messages:
                    is_forwarding = False
                    break

        except Exception as e:
            await status.edit_text(f"❌ Error: `{e}`", reply_markup=None)
            is_forwarding = False
            return

        await status.edit_text(
            f"🎉 **Movie Forwarding Complete!**\n"
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

    # General Stats
    total_in_fwd_db = 0
    if os.path.exists(DUPLICATE_DB_FILE):
        with open(DUPLICATE_DB_FILE, "r") as f:
            total_in_fwd_db = len(f.readlines())
            
    await message.reply(
        f"📊 **Bot Status**\n\n"
        f"--- **Session** ---\n"
        f"Forwarded: `{forwarded_count}`\n"
        f"Mode: `{'COPY' if mode_copy else 'FORWARD'}`\n"
        f"Movie Target: `{target_channel}`\n"
        f"Movie Limit: `{limit_messages}`\n"
        f"Bot-Forwarded DB: `{total_in_fwd_db}` entries\n"
        f"--- **🎬 Movies** ---\n"
        f"Indexed Source Movies: `{total_in_movie_index}`\n"
        f"Indexed Target (IDs): `{total_in_target_movie_ids}`\n"
        f"Indexed Target (Name+Size): `{total_in_target_movie_comp_keys}`\n"
        f"--- **📺 Web Series** ---\n"
        f"Indexed Source Episodes: `{total_in_webseries_index}`\n"
        f"Indexed Target (IDs): `{total_in_target_ws_ids}`\n"
        f"Indexed Target (Name+Size): `{total_in_target_ws_comp_keys}`"
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
print("Loading databases (Movie & Web Series Mode)...")
load_movie_duplicate_dbs()
load_webseries_duplicate_dbs()
load_movie_index_db()
print(f"Loaded {len(movie_fwd_unique_ids)} total movie unique IDs into memory.")
print(f"Loaded {len(webseries_fwd_unique_ids)} total webseries unique IDs into memory.")
print(f"Loaded {len(movie_index.get('movies', {}))} indexed movies from {MOVIE_INDEX_DB_FILE}")
print("✅ UserBot ready — send commands.")
app.run()
