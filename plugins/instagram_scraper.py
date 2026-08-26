import os
import sys
import time
import json
import random
import logging
import requests
import threading
import asyncio
from datetime import datetime

# Telebot imports
from telebot.types import InputMediaPhoto, InputMediaVideo

# Setup Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("instagram_scraper_plugin")

# Global variables to prevent NameError in background threads
bot = None
user_states = {}
is_standalone = True
running_userbots = {}
main_module = None

# Optional PostgreSQL support
try:
    import psycopg2
except ImportError:
    psycopg2 = None

# --- DATABASE MANAGER FOR BOTH POSTGRESQL & SQLITE ---
class InstagramDB:
    def __init__(self):
        self.db_url = os.environ.get("DATABASE_URL")
        self.is_postgres = False
        self.init_db()

    def get_connection(self):
        if self.db_url:
            try:
                url = self.db_url
                if url.startswith("postgres://"):
                    url = url.replace("postgres://", "postgresql://", 1)
                if psycopg2 is not None:
                    conn = psycopg2.connect(url)
                    self.is_postgres = True
                    return conn
                else:
                    logger.warning("psycopg2 is not installed. Falling back to SQLite.")
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}. Falling back to SQLite.")
        
        self.is_postgres = False
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sqlite_file = os.path.join(base_dir, "bot.db")
        if not os.path.exists(sqlite_file):
            parent_dir = os.path.dirname(base_dir)
            parent_sqlite = os.path.join(parent_dir, "bot.db")
            if os.path.exists(parent_sqlite):
                sqlite_file = parent_sqlite
            else:
                if not os.path.exists(os.path.join(base_dir, "bot.py")) and os.path.exists(os.path.join(parent_dir, "bot.py")):
                    sqlite_file = os.path.join(parent_dir, "bot.db")
                else:
                    sqlite_file = os.path.join(base_dir, "instagram.db")
            
        import sqlite3
        return sqlite3.connect(sqlite_file)

    def execute_query(self, query, params=(), commit=False, fetch=None, suppress_errors=False):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            if not self.is_postgres:
                # SQLite uses ? instead of %s
                query = query.replace("%s", "?")
            cursor.execute(query, params)
            if commit:
                conn.commit()
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
        except Exception as e:
            if not suppress_errors:
                logger.error(f"Database query error: {e}. Query: {query}")
            if commit:
                try:
                    conn.rollback()
                except:
                    pass
        finally:
            cursor.close()
            conn.close()

    def table_exists(self, table_name):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if self.is_postgres:
                cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
                    (table_name.lower(),)
                )
                row = cursor.fetchone()
                exists = row[0] if row else False
            else:
                cursor.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name.lower(),)
                )
                row = cursor.fetchone()
                exists = (row[0] > 0) if row else False
            cursor.close()
            conn.close()
            return exists
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False

    def init_db(self):
        # Create target profiles table
        if self.db_url and psycopg2 is not None:
            # PostgreSQL syntax
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_targets (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100) UNIQUE NOT NULL,
                    full_name TEXT,
                    biography TEXT,
                    profile_pic_url TEXT,
                    followers_count INTEGER DEFAULT 0,
                    following_count INTEGER DEFAULT 0,
                    posts_count INTEGER DEFAULT 0,
                    last_refreshed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ia_stories INTEGER DEFAULT 0,
                    ia_interval INTEGER DEFAULT 15,
                    last_auto_poll_time VARCHAR(100) DEFAULT '0',
                    ia_posts INTEGER DEFAULT 0,
                    ia_post_interval INTEGER DEFAULT 15,
                    last_auto_post_poll_time VARCHAR(100) DEFAULT '0'
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_settings (
                    key VARCHAR(100) PRIMARY KEY,
                    value TEXT
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_seen_stories (
                    story_id VARCHAR(100) PRIMARY KEY,
                    username VARCHAR(100),
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_seen_posts (
                    post_id VARCHAR(100) PRIMARY KEY,
                    username VARCHAR(100),
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_target_receivers (
                    target_id INTEGER,
                    telegram_id VARCHAR(100),
                    name TEXT,
                    receiver_type VARCHAR(50),
                    stories_enabled INTEGER DEFAULT 0,
                    posts_enabled INTEGER DEFAULT 0,
                    PRIMARY KEY (target_id, telegram_id)
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_search_posts (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100),
                    post_id VARCHAR(100),
                    media_url TEXT,
                    media_type VARCHAR(20),
                    caption TEXT,
                    likes_count INTEGER DEFAULT 0,
                    comments_count INTEGER DEFAULT 0,
                    taken_at INTEGER
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_api_keys (
                    id SERIAL PRIMARY KEY,
                    api_key VARCHAR(255) UNIQUE NOT NULL,
                    provider VARCHAR(100) DEFAULT 'instagram120',
                    host VARCHAR(255) DEFAULT 'instagram120.p.rapidapi.com',
                    requests_count INTEGER DEFAULT 0,
                    active INTEGER DEFAULT 1
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_search_profiles (
                    username VARCHAR(100) PRIMARY KEY,
                    full_name TEXT,
                    biography TEXT,
                    profile_pic_url TEXT,
                    followers_count INTEGER DEFAULT 0,
                    following_count INTEGER DEFAULT 0,
                    posts_count INTEGER DEFAULT 0,
                    last_searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """, commit=True)
        else:
            # SQLite syntax
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    full_name TEXT,
                    biography TEXT,
                    profile_pic_url TEXT,
                    followers_count INTEGER DEFAULT 0,
                    following_count INTEGER DEFAULT 0,
                    posts_count INTEGER DEFAULT 0,
                    last_refreshed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    ia_stories INTEGER DEFAULT 0,
                    ia_interval INTEGER DEFAULT 15,
                    last_auto_poll_time TEXT DEFAULT '0',
                    ia_posts INTEGER DEFAULT 0,
                    ia_post_interval INTEGER DEFAULT 15,
                    last_auto_post_poll_time TEXT DEFAULT '0'
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_seen_stories (
                    story_id TEXT PRIMARY KEY,
                    username TEXT,
                    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_seen_posts (
                    post_id TEXT PRIMARY KEY,
                    username TEXT,
                    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_target_receivers (
                    target_id INTEGER,
                    telegram_id TEXT,
                    name TEXT,
                    receiver_type TEXT,
                    stories_enabled INTEGER DEFAULT 0,
                    posts_enabled INTEGER DEFAULT 0,
                    PRIMARY KEY (target_id, telegram_id)
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_search_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    post_id TEXT,
                    media_url TEXT,
                    media_type TEXT,
                    caption TEXT,
                    likes_count INTEGER DEFAULT 0,
                    comments_count INTEGER DEFAULT 0,
                    taken_at INTEGER
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_key TEXT UNIQUE NOT NULL,
                    provider TEXT DEFAULT 'instagram120',
                    host TEXT DEFAULT 'instagram120.p.rapidapi.com',
                    requests_count INTEGER DEFAULT 0,
                    active INTEGER DEFAULT 1
                )
            """, commit=True)
            self.execute_query("""
                CREATE TABLE IF NOT EXISTS instagram_search_profiles (
                    username TEXT PRIMARY KEY,
                    full_name TEXT,
                    biography TEXT,
                    profile_pic_url TEXT,
                    followers_count INTEGER DEFAULT 0,
                    following_count INTEGER DEFAULT 0,
                    posts_count INTEGER DEFAULT 0,
                    last_searched_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """, commit=True)
            
        # Add defensive migrations for columns if table already exists
        for col_name, col_def in [
            ("ia_stories", "INTEGER DEFAULT 0"),
            ("ia_interval", "INTEGER DEFAULT 15"),
            ("last_auto_poll_time", "VARCHAR(100) DEFAULT '0'"),
            ("ia_posts", "INTEGER DEFAULT 0"),
            ("ia_post_interval", "INTEGER DEFAULT 15"),
            ("last_auto_post_poll_time", "VARCHAR(100) DEFAULT '0'")
        ]:
            try:
                self.execute_query(f"ALTER TABLE instagram_targets ADD COLUMN {col_name} {col_def}", commit=True, suppress_errors=True)
            except Exception:
                pass
            
        # Seed default settings
        self.seed_setting_default("story_polling_interval", "15")
        self.seed_setting_default("notification_chat_id", "")
        self.seed_setting_default("last_auto_poll_time", "0")
        
        # Auto-seed legacy environment key if it exists and keys table is empty
        try:
            key_count = self.execute_query("SELECT count(*) FROM instagram_api_keys", fetch="one")
            if key_count and key_count[0] == 0:
                legacy_key = os.environ.get("RAPIDAPI_KEY")
                if not legacy_key:
                    try:
                        row = self.execute_query("SELECT value FROM settings WHERE key = %s", ("config",), fetch="one")
                        if row:
                            config = json.loads(row[0])
                            legacy_key = config.get("RAPIDAPI_KEY")
                    except:
                        pass
                if legacy_key and legacy_key.strip():
                    self.execute_query(
                        "INSERT INTO instagram_api_keys (api_key, provider, host) VALUES (%s, %s, %s)",
                        (legacy_key.strip(), "instagram120", "instagram120.p.rapidapi.com"),
                        commit=True
                    )
                    logger.info("Seeded legacy env API Key into database.")
        except Exception as e:
            logger.error(f"Error seeding legacy key: {e}")
            
        logger.info("Instagram Plugin Database Initialized.")

    def seed_setting_default(self, key, val):
        row = self.execute_query("SELECT 1 FROM instagram_settings WHERE key = %s", (key,), fetch="one")
        if not row:
            self.execute_query("INSERT INTO instagram_settings (key, value) VALUES (%s, %s)", (key, val), commit=True)


db_client = InstagramDB()


# --- SETTINGS GETTERS & SETTERS ---
def get_instagram_setting(key, default=None):
    row = db_client.execute_query("SELECT value FROM instagram_settings WHERE key = %s", (key,), fetch="one")
    if row:
        return row[0]
    return default

def set_instagram_setting(key, value):
    # Try updating
    db_client.execute_query("UPDATE instagram_settings SET value = %s WHERE key = %s", (str(value), key), commit=True)
    # Check if exists
    row = db_client.execute_query("SELECT 1 FROM instagram_settings WHERE key = %s", (key,), fetch="one")
    if not row:
        db_client.execute_query("INSERT INTO instagram_settings (key, value) VALUES (%s, %s)", (key, str(value)), commit=True)


def resolve_bot_username_robust(bot_instance=None):
    # 1. Check DB first
    uname = get_instagram_setting("bot_username")
    if uname:
        return uname
        
    # 2. Try bot_instance or global bot
    b = bot_instance or bot
    if not b:
        for mod_name in ["__main__", "bot", "main"]:
            mod = sys.modules.get(mod_name)
            if mod:
                b = getattr(mod, "bot", None) or getattr(mod, "app", None)
                if b:
                    break
    if b:
        try:
            me = b.get_me()
            if me and me.username:
                set_instagram_setting("bot_username", me.username)
                return me.username
        except Exception as e:
            logger.error(f"Failed to get_me from bot dynamically: {e}")
            
    # 3. Fallback to parsing from env or config.json
    temp_token = (
        os.environ.get("BOT_TOKEN") or
        os.environ.get("TELEGRAM_BOT_TOKEN") or
        os.environ.get("TELEGRAM_TOKEN") or
        os.environ.get("TOKEN")
    )
    if not temp_token:
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(os.path.dirname(base_dir), "config.json")
            if not os.path.exists(config_path):
                config_path = os.path.join(os.path.dirname(os.path.dirname(base_dir)), "config.json")
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                    temp_token = cfg.get("bot_token")
        except:
            pass
            
    if temp_token and "YOUR_BOT_TOKEN" not in temp_token and ":" in temp_token:
        try:
            from telebot import TeleBot
            temp_bot = TeleBot(temp_token.strip())
            me = temp_bot.get_me()
            if me and me.username:
                set_instagram_setting("bot_username", me.username)
                return me.username
        except Exception as e:
            logger.error(f"Failed to resolve username via temp bot token: {e}")
            
    return None


# --- CONFIG & RAPIDAPI LEGACY KEY RESOLUTION ---
def get_setting(key, default=None):
    """Fetches a setting from environment variable or main bot settings config JSON."""
    val = os.environ.get(key)
    if val:
        return val
        
    try:
        if db_client.table_exists("settings"):
            row = db_client.execute_query("SELECT value FROM settings WHERE key = %s", ("config",), fetch="one")
            if row:
                config = json.loads(row[0])
                if key in config:
                    return config[key]
    except Exception:
        pass
        
    return default


def get_rapidapi_key():
    return get_setting("RAPIDAPI_KEY")


def find_cursor_in_dict(d, depth=0):
    if depth > 3:
        return None
    if not isinstance(d, dict):
        return None
        
    preferred_keys = [
        "next_max_id", "next_cursor", "end_cursor", "next_page_token", "cursor",
        "nextPage", "next_page", "next", "after"
    ]
    for k in preferred_keys:
        val = d.get(k)
        if val and isinstance(val, (str, int)):
            return str(val)
            
    nested_keys = ["page_info", "pagination", "paging", "cursors"]
    for nk in nested_keys:
        nd = d.get(nk)
        if isinstance(nd, dict):
            val = find_cursor_in_dict(nd, depth + 1)
            if val:
                return val
                
    for k, v in d.items():
        k_lower = k.lower()
        if any(x in k_lower for x in ["cursor", "max_id", "page_token", "next"]):
            if isinstance(v, (str, int)) and v:
                return str(v)
            elif isinstance(v, dict):
                val = find_cursor_in_dict(v, depth + 1)
                if val:
                    return val
    return None


class HikerAPIScraper:
    @staticmethod
    def parse_timestamp(val):
        if not val:
            return int(time.time())
        try:
            return int(float(val))
        except (ValueError, TypeError):
            try:
                from datetime import datetime
                clean_val = str(val).replace("Z", "+00:00")
                dt = datetime.fromisoformat(clean_val)
                return int(dt.timestamp())
            except Exception as e:
                logger.warning(f"Failed parsing HikerAPI timestamp {val}: {e}")
                return int(time.time())

    @staticmethod
    def _headers(api_key):
        return {
            "x-access-key": api_key,
            "Accept": "application/json"
        }

    @classmethod
    def get_user_info(cls, username, api_key, host=None):
        username_clean = username.lstrip("@").strip()
        url = f"https://api.hikerapi.com/v1/user/by/username?username={username_clean}"
        response = requests.get(url, headers=cls._headers(api_key), timeout=90)
        if response.status_code != 200:
            raise RuntimeError(f"HikerAPI user info failed: Status {response.status_code}")
            
        data = response.json()
        if not data or not isinstance(data, dict):
            raise RuntimeError("HikerAPI returned empty profile data")
            
        return {
            "id": str(data.get("pk", "")),
            "username": data.get("username", username_clean),
            "full_name": data.get("full_name", ""),
            "biography": data.get("biography", ""),
            "profile_pic_url": data.get("profile_pic_url", ""),
            "followers_count": data.get("follower_count", 0),
            "following_count": data.get("following_count", 0),
            "posts_count": data.get("media_count", 0)
        }

    @classmethod
    def get_latest_posts(cls, username, api_key, host=None, cursor=None):
        username_clean = username.lstrip("@").strip()
        
        info = cls.get_user_info(username_clean, api_key)
        user_id = info["id"]
        
        url = f"https://api.hikerapi.com/v1/user/medias/chunk?user_id={user_id}"
        params = {}
        if cursor:
            params["end_cursor"] = cursor
            
        response = requests.get(url, params=params, headers=cls._headers(api_key), timeout=90)
        if response.status_code != 200:
            raise RuntimeError(f"HikerAPI posts failed: Status {response.status_code}")
            
        res_json = response.json()
        items = res_json.get("items", []) or []
        next_c = res_json.get("end_cursor")
        
        posts = []
        for item in items:
            if not isinstance(item, dict):
                continue
            post_id = item.get("id") or item.get("pk")
            caption_dict = item.get("caption") or {}
            caption = caption_dict.get("text", "") if isinstance(caption_dict, dict) else str(caption_dict)
            likes = item.get("like_count", 0)
            comments = item.get("comment_count", 0)
            taken_at = cls.parse_timestamp(item.get("taken_at"))
            
            media_url = ""
            media_type = "image"
            media_type_raw = item.get("media_type", 1)
            
            if item.get("video_versions"):
                media_type = "video"
                media_url = item["video_versions"][0].get("url")
            elif item.get("image_versions2"):
                candidates = item["image_versions2"].get("candidates", [])
                if candidates:
                    media_url = candidates[0].get("url")
            if not media_url:
                media_url = item.get("thumbnail_url") or ""
                
            if media_url and ("mp4" in media_url or media_type_raw == 2):
                media_type = "video"
                
            if post_id and media_url:
                posts.append({
                    "id": str(post_id),
                    "media_url": media_url,
                    "media_type": media_type,
                    "caption": caption,
                    "taken_at": taken_at,
                    "likes_count": int(likes),
                    "comments_count": int(comments)
                })
        return posts, next_c

    @classmethod
    def get_latest_stories(cls, username, api_key, host=None):
        username_clean = username.lstrip("@").strip()
        
        # 1. Resolve user ID first to ensure stories fetch is reliable
        try:
            info = cls.get_user_info(username_clean, api_key)
            user_id = info["id"]
        except Exception as e:
            logger.error(f"HikerAPIScraper: failed to resolve user ID for {username_clean}: {e}")
            set_instagram_setting("last_stories_debug", f"HikerAPI: Failed to resolve user ID: {e}")
            return []
            
        # 2. Query stories by user ID using v1/user/stories
        url = f"https://api.hikerapi.com/v1/user/stories"
        params = {"user_id": user_id}
        response = requests.get(url, params=params, headers=cls._headers(api_key), timeout=90)
        
        if response.status_code == 404:
            set_instagram_setting("last_stories_debug", "HikerAPI: 404 Not Found (user has no active stories)")
            return []
        if response.status_code != 200:
            set_instagram_setting("last_stories_debug", f"HikerAPI: Request failed with status {response.status_code}")
            raise RuntimeError(f"HikerAPI stories failed: Status {response.status_code}")
            
        res_json = response.json()
        items = []
        if isinstance(res_json, list):
            items = res_json
        elif isinstance(res_json, dict):
            items = res_json.get("items", []) or res_json.get("stories", [])
            
        if not items:
            set_instagram_setting("last_stories_debug", "HikerAPI: Response items list was empty")
        else:
            sample = items[0] if len(items) > 0 else {}
            sample_keys = list(sample.keys()) if isinstance(sample, dict) else []
            set_instagram_setting("last_stories_debug", f"HikerAPI: Fetched {len(items)} items. Sample keys: {sample_keys}")
            
        stories = []
        for item in items:
            if not isinstance(item, dict):
                continue
            story_id = item.get("id") or item.get("pk")
            taken_at = cls.parse_timestamp(item.get("taken_at"))
            
            media_url = ""
            media_type = "image"
            media_type_raw = item.get("media_type", 1)
            
            if item.get("video_versions"):
                media_type = "video"
                media_url = item["video_versions"][0].get("url")
            elif item.get("image_versions2"):
                candidates = item["image_versions2"].get("candidates", [])
                if candidates:
                    media_url = candidates[0].get("url")
                    
            if not media_url:
                media_url = item.get("thumbnail_url") or ""
                
            if media_url and ("mp4" in media_url or media_type_raw == 2):
                media_type = "video"
                
            if story_id and media_url:
                stories.append({
                    "id": str(story_id),
                    "media_url": media_url,
                    "media_type": media_type,
                    "taken_at": taken_at
                })
        return stories


# --- SCRAPER PROVIDER ADAPTERS ---
class MockScraper:
    @staticmethod
    def get_user_info(username, api_key=None, host=None):
        username_clean = username.lstrip("@").lower()
        return {
            "username": username_clean,
            "full_name": f"{username_clean.title()} | Fan Account",
            "biography": f"✨ Loving life and sharing photos! Tracked profile for @{username_clean}.",
            # High-resolution Unsplash photo placeholder (640x640)
            "profile_pic_url": "https://images.unsplash.com/photo-1534528741775-53994a69daeb?w=640&auto=format&fit=crop&q=80",
            "followers_count": random.randint(15000, 2500000),
            "following_count": random.randint(200, 1200),
            "posts_count": random.randint(45, 890)
        }

    @staticmethod
    def get_latest_stories(username, api_key=None, host=None):
        now = int(time.time())
        stories = []
        stories.append({
            "id": f"story_mock_img_{random.randint(100,999)}",
            "media_url": random.choice(MOCK_IMAGES),
            "media_type": "image",
            "taken_at": now - 3600
        })
        stories.append({
            "id": f"story_mock_vid_{random.randint(100,999)}",
            "media_url": MOCK_VIDEOS[0],
            "media_type": "video",
            "taken_at": now - 7200
        })
        return stories

    @staticmethod
    def get_latest_posts(username, api_key=None, host=None, cursor=None):
        posts = []
        now = int(time.time())
        captions = [
            "Cozy morning coffee vibe ☕✨ #morning",
            "Working on some updates! Coding late nights 💻👾 #developer",
            "Nature is beautiful! Golden hour is best 🌅⛰️ #scenery",
            "Success is built daily! Keep grinding grinds 💪🔥 #motivation",
            "Workspace inspiration! Sleek desk setup ⌨️🖱️ #tech",
            "Weekend hiking adventure 🥾🏔️ #explore",
            "Loving this amazing book 📖☕ #aesthetic"
        ]
        
        # Generate 30 mock posts for pagination tests
        start_idx = 0 if cursor is None else 30
        for i in range(start_idx, start_idx + 30):
            is_video = (i % 3 == 0)
            media_url = MOCK_VIDEOS[0] if is_video else MOCK_IMAGES[i % len(MOCK_IMAGES)]
            posts.append({
                "id": f"post_mock_{i+1}_{random.randint(100,999)}",
                "media_url": media_url,
                "media_type": "video" if is_video else "image",
                "caption": f"Mock post {i+1}: {captions[i % len(captions)]}",
                "taken_at": now - (3600 * (i + 1)),
                "likes_count": random.randint(15, 1200),
                "comments_count": random.randint(2, 89)
            })
        next_c = "mock_cursor_30" if cursor is None else None
        return posts, next_c


class RapidAPIScraper:
    @staticmethod
    def _headers(api_key, host):
        return {
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": host,
            "Content-Type": "application/json"
        }

    @classmethod
    def get_user_info(cls, username, api_key, host):
        username_clean = username.lstrip("@").strip()
        is_get_api = any(x in host for x in ["rocketapi", "scraper-api2", "data12", "bulk-scraper"])
        
        if "instagram-best-experience" in host:
            url = f"https://{host}/profile?username={username_clean}"
            response = requests.get(url, headers=cls._headers(api_key, host), timeout=90)
        elif is_get_api:
            url = f"https://{host}/v1/info?username_or_id_or_url={username_clean}"
            if "data12" in host:
                url = f"https://{host}/user/info?username={username_clean}"
            response = requests.get(url, headers=cls._headers(api_key, host), timeout=90)
        else:
            url = f"https://{host}/api/instagram/profile"
            response = requests.post(url, headers=cls._headers(api_key, host), json={"username": username_clean}, timeout=90)
            
        if response.status_code != 200:
            raise RuntimeError(f"API request failed with status {response.status_code}")
            
        res_json = response.json()
        
        if "instagram-best-experience" in host:
            followers = int(res_json.get("follower_count") or 0)
            following = int(res_json.get("following_count") or 0)
            posts_count = int(res_json.get("media_count") or 0)
            
            pic_url = ""
            hd_info = res_json.get("hd_profile_pic_url_info")
            if isinstance(hd_info, dict):
                pic_url = hd_info.get("url")
            if not pic_url:
                pic_url = res_json.get("profile_pic_url") or ""
                
            return {
                "username": res_json.get("username", username_clean),
                "full_name": res_json.get("full_name", ""),
                "biography": res_json.get("biography", ""),
                "profile_pic_url": pic_url,
                "followers_count": followers,
                "following_count": following,
                "posts_count": posts_count
            }

        data = res_json.get("data", {})
        user = data.get("user", {}) if isinstance(data, dict) else {}
        if not user and isinstance(res_json, dict):
            user = res_json.get("result", {}) or res_json.get("user", {}) or res_json
            
        followers = user.get("follower_count") or user.get("followers_count")
        if not followers and isinstance(user.get("edge_followed_by"), dict):
            followers = user["edge_followed_by"].get("count")
        followers = int(followers or 0)
        
        following = user.get("following_count") or user.get("following")
        if not following and isinstance(user.get("edge_follow"), dict):
            following = user["edge_follow"].get("count")
        following = int(following or 0)
        
        posts_count = user.get("media_count") or user.get("posts_count")
        if not posts_count and isinstance(user.get("edge_owner_to_timeline_media"), dict):
            posts_count = user["edge_owner_to_timeline_media"].get("count")
        posts_count = int(posts_count or 0)
        
        # --- EXTRACT HIGH QUALITY HD PROFILE PHOTO RESOLUTION ---
        pic_url = ""
        hd_candidates = user.get("hd_profile_pic_versions") or user.get("hd_profile_pic_info_dict")
        if isinstance(hd_candidates, list) and len(hd_candidates) > 0:
            pic_url = hd_candidates[0].get("url")
        elif isinstance(hd_candidates, dict):
            pic_url = hd_candidates.get("url")
            
        if not pic_url:
            pic_url = user.get("profile_pic_url_hd") or user.get("profile_pic_url") or ""
            
        return {
            "username": user.get("username", username_clean),
            "full_name": user.get("full_name", ""),
            "biography": user.get("biography", ""),
            "profile_pic_url": pic_url,
            "followers_count": followers,
            "following_count": following,
            "posts_count": posts_count
        }

    @classmethod
    def get_latest_stories(cls, username, api_key, host):
        username_clean = username.lstrip("@").strip()
        is_get_api = any(x in host for x in ["rocketapi", "scraper-api2", "data12", "bulk-scraper"])
        
        if "instagram-best-experience" in host:
            prof_url = f"https://{host}/profile?username={username_clean}"
            prof_res = requests.get(prof_url, headers=cls._headers(api_key, host), timeout=90)
            if prof_res.status_code != 200:
                raise RuntimeError(f"Failed to fetch profile ID (Status {prof_res.status_code})")
            
            prof_data = prof_res.json()
            user_id = prof_data.get("pk") or prof_data.get("id") or prof_data.get("user_id")
            if not user_id and isinstance(prof_data.get("data"), dict):
                user_id = prof_data["data"].get("id") or prof_data["data"].get("pk")
                
            if not user_id:
                raise RuntimeError(f"User ID not found in profile response for {username_clean}")
                
            url = f"https://{host}/stories?user_id={user_id}"
            response = requests.get(url, headers=cls._headers(api_key, host), timeout=90)
        elif is_get_api:
            url = f"https://{host}/v1/stories?username_or_id_or_url={username_clean}"
            if "data12" in host:
                url = f"https://{host}/user/stories?username={username_clean}"
            response = requests.get(url, headers=cls._headers(api_key, host), timeout=90)
        else:
            url = f"https://{host}/api/instagram/stories"
            response = requests.post(url, headers=cls._headers(api_key, host), json={"username": username_clean}, timeout=90)
            
        if response.status_code == 404:
            return []
        if response.status_code != 200:
            raise RuntimeError(f"API stories failed with status {response.status_code}")
            
        res_json = response.json()
        items = []
        
        if "instagram-best-experience" in host:
            if isinstance(res_json, list):
                items = res_json
            elif isinstance(res_json, dict):
                items = res_json.get("items", []) or res_json.get("stories", [])
        else:
            data = res_json.get("data", {})
            if isinstance(data, dict):
                reels = data.get("reels_media", [])
                if reels and isinstance(reels, list) and len(reels) > 0:
                    items = reels[0].get("items", [])
                else:
                    user = data.get("user", {})
                    if isinstance(user, dict):
                        reel = user.get("reel", {})
                        if isinstance(reel, dict):
                            items = reel.get("items", [])
                            
            if not items:
                if isinstance(res_json.get("result"), list):
                    items = res_json["result"]
                elif isinstance(res_json.get("items"), list):
                    items = res_json["items"]
                elif isinstance(data, list):
                    items = data
                elif isinstance(res_json.get("stories"), list):
                    items = res_json["stories"]
                
        if not items:
            set_instagram_setting("last_stories_debug", f"RapidAPI: No items found in response (status {response.status_code})")
        else:
            sample = items[0] if len(items) > 0 else {}
            sample_keys = list(sample.keys()) if isinstance(sample, dict) else []
            set_instagram_setting("last_stories_debug", f"RapidAPI: Fetched {len(items)} items. Sample keys: {sample_keys}")
            
        stories = []
        for item in items:
            story_id = item.get("id") or item.get("pk")
            taken_at = item.get("taken_at") or item.get("taken_at_timestamp") or int(time.time())
            
            media_url = ""
            media_type = "image"
            media_type_raw = item.get("media_type", 1)
            
            if item.get("video_versions"):
                media_type = "video"
                media_url = item["video_versions"][0].get("url")
            elif item.get("video_url"):
                media_type = "video"
                media_url = item["video_url"]
            elif item.get("image_versions2"):
                candidates = item["image_versions2"].get("candidates", [])
                if candidates:
                    media_url = candidates[0].get("url")
            
            if not media_url:
                media_url = item.get("image_url") or item.get("display_url") or item.get("display_src") or ""
                
            if media_url and ("mp4" in media_url or media_type_raw == 2):
                media_type = "video"
                
            if story_id and media_url:
                stories.append({
                    "id": str(story_id),
                    "media_url": media_url,
                    "media_type": media_type,
                    "taken_at": int(taken_at)
                })
        return stories

    @classmethod
    def get_latest_posts(cls, username, api_key, host, cursor=None):
        username_clean = username.lstrip("@").strip()
        is_get_api = any(x in host for x in ["rocketapi", "scraper-api2", "data12", "bulk-scraper"])
        
        all_posts = []
        current_cursor = cursor
        
        # Loop to aggregate posts (min 30, max 100 posts per call batch)
        # Limit to 5 requests maximum to prevent rate limits / heavy quota charges
        max_requests = 5
        
        user_id = None
        if "instagram-best-experience" in host:
            prof_url = f"https://{host}/profile?username={username_clean}"
            prof_res = requests.get(prof_url, headers=cls._headers(api_key, host), timeout=90)
            if prof_res.status_code != 200:
                raise RuntimeError(f"Failed to fetch profile ID (Status {prof_res.status_code})")
            
            prof_data = prof_res.json()
            user_id = prof_data.get("pk") or prof_data.get("id") or prof_data.get("user_id")
            if not user_id and isinstance(prof_data.get("data"), dict):
                user_id = prof_data["data"].get("id") or prof_data["data"].get("pk")
                
            if not user_id:
                raise RuntimeError(f"User ID not found in profile response for {username_clean}")
                
        for attempt in range(max_requests):
            if "instagram-best-experience" in host:
                url = f"https://{host}/feed?user_id={user_id}"
                if current_cursor:
                    url += f"&max_id={current_cursor}&cursor={current_cursor}"
                response = requests.get(url, headers=cls._headers(api_key, host), timeout=90)
            elif is_get_api:
                url_args = f"username_or_id_or_url={username_clean}&count=100&limit=100"
                if current_cursor:
                    url_args += f"&cursor={current_cursor}&max_id={current_cursor}&next_max_id={current_cursor}&pagination_token={current_cursor}"
                url = f"https://{host}/v1/posts?{url_args}"
                if "data12" in host:
                    url = f"https://{host}/user/posts?username={username_clean}&count=100&limit=100"
                    if current_cursor:
                        url += f"&cursor={current_cursor}&max_id={current_cursor}"
                response = requests.get(url, headers=cls._headers(api_key, host), timeout=90)
            else:
                url = f"https://{host}/api/instagram/posts"
                payload = {"username": username_clean, "count": 100, "limit": 100}
                if current_cursor:
                    payload["cursor"] = current_cursor
                    payload["max_id"] = current_cursor
                    payload["next_max_id"] = current_cursor
                    payload["pagination_token"] = current_cursor
                response = requests.post(url, headers=cls._headers(api_key, host), json=payload, timeout=90)
                
            if response.status_code != 200:
                if attempt == 0:
                    raise RuntimeError(f"API posts fetch failed: Status {response.status_code}")
                else:
                    break
                    
            res_json = response.json()
            result = res_json.get("result", {}) or res_json
            
            edges = result.get("edges", [])
            if not edges and isinstance(result, dict):
                media_data = result.get("edge_owner_to_timeline_media", {})
                if isinstance(media_data, dict):
                    edges = media_data.get("edges", [])
                    
            posts = []
            if edges:
                for edge in edges:
                    node = edge.get("node", {})
                    if not node:
                        continue
                    post_id = node.get("id") or node.get("pk")
                    taken_at = node.get("taken_at_timestamp") or node.get("taken_at") or int(time.time())
                    caption = ""
                    cap_edges = node.get("edge_media_to_caption", {}).get("edges", [])
                    if cap_edges and len(cap_edges) > 0:
                        caption = cap_edges[0].get("node", {}).get("text", "")
                    if not caption:
                        caption = node.get("caption", "")
                        if isinstance(caption, dict):
                            caption = caption.get("text", "")
                    # Sidecar / Carousel parsing
                    is_video = (
                        node.get("is_video") is True or
                        node.get("media_type") == 2 or
                        node.get("media_type_raw") == 2 or
                        str(node.get("product_type", "")).lower() in ["clips", "reels"] or
                        bool(node.get("video_versions")) or
                        bool(node.get("video_url")) or
                        node.get("__typename") == "GraphVideo"
                    )
                    
                    sidecar = node.get("edge_sidecar_to_children", {})
                    child_edges = sidecar.get("edges", []) if isinstance(sidecar, dict) else []
                    carousel = node.get("carousel_media", [])
                    
                    if (child_edges or (carousel and isinstance(carousel, list) and len(carousel) > 0)) and not is_video:
                        if child_edges:
                            for child_edge in child_edges:
                                child_node = child_edge.get("node", {})
                                if not child_node:
                                    continue
                                child_is_video = (
                                    child_node.get("is_video") is True or
                                    child_node.get("media_type") == 2 or
                                    bool(child_node.get("video_versions")) or
                                    bool(child_node.get("video_url"))
                                )
                                child_media_type = "video" if child_is_video else "image"
                                child_url = ""
                                if child_is_video:
                                    if child_node.get("video_url"):
                                        child_url = child_node.get("video_url")
                                    elif child_node.get("video_versions"):
                                        v_vers = child_node.get("video_versions")
                                        if isinstance(v_vers, list) and len(v_vers) > 0:
                                            child_url = v_vers[0].get("url")
                                        elif isinstance(v_vers, dict):
                                            child_url = v_vers.get("url")
                                if not child_url:
                                    child_url = child_node.get("display_url")
                                if not child_url and child_node.get("image_versions2"):
                                    candidates = child_node["image_versions2"].get("candidates", [])
                                    if candidates:
                                        child_url = candidates[0].get("url")
                                if child_url:
                                    posts.append({
                                        "id": str(post_id),
                                        "media_url": child_url,
                                        "media_type": child_media_type,
                                        "caption": str(caption),
                                        "taken_at": int(taken_at),
                                        "likes_count": node.get("like_count") or node.get("likes_count") or 0,
                                        "comments_count": node.get("comment_count") or node.get("comments_count") or 0
                                    })
                        elif carousel:
                            for sub_item in carousel:
                                sub_type_raw = sub_item.get("media_type", 1)
                                sub_is_video = (sub_type_raw == 2 or bool(sub_item.get("video_versions")) or bool(sub_item.get("video_url")))
                                sub_media_type = "video" if sub_is_video else "image"
                                sub_url = ""
                                if sub_is_video:
                                    if sub_item.get("video_versions"):
                                        v_vers = sub_item.get("video_versions")
                                        if isinstance(v_vers, list) and len(v_vers) > 0:
                                            sub_url = v_vers[0].get("url")
                                        elif isinstance(v_vers, dict):
                                            sub_url = v_vers.get("url")
                                    elif sub_item.get("video_url"):
                                        sub_url = sub_item.get("video_url")
                                if not sub_url and sub_item.get("image_versions2"):
                                    candidates = sub_item["image_versions2"].get("candidates", [])
                                    if candidates:
                                        sub_url = candidates[0].get("url")
                                if not sub_url:
                                    sub_url = sub_item.get("image_url") or sub_item.get("display_url") or ""
                                if sub_url:
                                    posts.append({
                                        "id": str(post_id),
                                        "media_url": sub_url,
                                        "media_type": sub_media_type,
                                        "caption": str(caption),
                                        "taken_at": int(taken_at),
                                        "likes_count": node.get("like_count") or node.get("likes_count") or 0,
                                        "comments_count": node.get("comment_count") or node.get("comments_count") or 0
                                    })
                    else:
                        media_type = "video" if is_video else "image"
                        media_url = ""
                        if is_video:
                            if node.get("video_url"):
                                media_url = node.get("video_url")
                            elif node.get("video_versions"):
                                v_vers = node.get("video_versions")
                                if isinstance(v_vers, list) and len(v_vers) > 0:
                                    media_url = v_vers[0].get("url")
                                elif isinstance(v_vers, dict):
                                    media_url = v_vers.get("url")
                        if not media_url:
                            media_url = node.get("display_url") or node.get("image_url") or node.get("display_uri") or ""
                        if not media_url and node.get("image_versions2"):
                            candidates = node["image_versions2"].get("candidates", [])
                            if candidates:
                                media_url = candidates[0].get("url")
                        likes = node.get("like_count") or node.get("likes_count") or 0
                        comments = node.get("comment_count") or node.get("comments_count") or 0
                        if post_id and media_url:
                            posts.append({
                                "id": str(post_id),
                                "media_url": media_url,
                                "media_type": media_type,
                                "caption": str(caption),
                                "taken_at": int(taken_at),
                                "likes_count": int(likes),
                                "comments_count": int(comments)
                            })
                            
            if not posts:
                items = result.get("items", []) or res_json.get("items", []) or res_json.get("posts", [])
                if isinstance(items, list):
                    for item in items:
                        post_id = item.get("id") or item.get("pk")
                        taken_at = item.get("taken_at") or item.get("taken_at_timestamp") or int(time.time())
                        caption_data = item.get("caption") or {}
                        caption = caption_data.get("text", "") if isinstance(caption_data, dict) else str(caption_data)
                        likes = item.get("like_count") or item.get("likes_count") or 0
                        comments = item.get("comment_count") or item.get("comments_count") or 0
                        
                        # Sidecar / Carousel parsing
                        media_type_raw = item.get("media_type", 1)
                        product_type = str(item.get("product_type", "")).lower()
                        is_video = (
                            media_type_raw == 2 or
                            product_type in ["reels", "clips"] or
                            item.get("is_video") is True or
                            bool(item.get("video_url")) or
                            bool(item.get("video_versions"))
                        )
                        carousel = item.get("carousel_media", [])
                        
                        if carousel and isinstance(carousel, list) and len(carousel) > 0 and not is_video:
                            for sub_item in carousel:
                                sub_type_raw = sub_item.get("media_type", 1)
                                sub_is_video = (sub_type_raw == 2 or bool(sub_item.get("video_versions")) or bool(sub_item.get("video_url")))
                                sub_media_type = "video" if sub_is_video else "image"
                                sub_url = ""
                                if sub_is_video:
                                    if sub_item.get("video_versions"):
                                        v_vers = sub_item.get("video_versions")
                                        if isinstance(v_vers, list) and len(v_vers) > 0:
                                            sub_url = v_vers[0].get("url")
                                        elif isinstance(v_vers, dict):
                                            sub_url = v_vers.get("url")
                                    elif sub_item.get("video_url"):
                                        sub_url = sub_item.get("video_url")
                                if not sub_url and sub_item.get("image_versions2"):
                                    candidates = sub_item["image_versions2"].get("candidates", [])
                                    if candidates:
                                        sub_url = candidates[0].get("url")
                                if not sub_url:
                                    sub_url = sub_item.get("image_url") or sub_item.get("display_url") or ""
                                if sub_url:
                                    posts.append({
                                        "id": str(post_id),
                                        "media_url": sub_url,
                                        "media_type": sub_media_type,
                                        "caption": caption,
                                        "taken_at": int(taken_at),
                                        "likes_count": int(likes),
                                        "comments_count": int(comments)
                                    })
                        else:
                            media_type = "video" if is_video else "image"
                            media_url = ""
                            if is_video:
                                if item.get("video_versions"):
                                    v_vers = item.get("video_versions")
                                    if isinstance(v_vers, list) and len(v_vers) > 0:
                                        media_url = v_vers[0].get("url")
                                    elif isinstance(v_vers, dict):
                                        media_url = v_vers.get("url")
                                elif item.get("video_url"):
                                    media_url = item.get("video_url")
                            if not media_url and item.get("image_versions2"):
                                candidates = item["image_versions2"].get("candidates", [])
                                if candidates:
                                    media_url = candidates[0].get("url")
                            if not media_url:
                                media_url = item.get("image_url") or item.get("display_url") or ""
                            if post_id and media_url:
                                posts.append({
                                    "id": str(post_id),
                                    "media_url": media_url,
                                    "media_type": media_type,
                                    "caption": caption,
                                    "taken_at": int(taken_at),
                                    "likes_count": int(likes),
                                    "comments_count": int(comments)
                                })
                                
            all_posts.extend(posts)
            logger.info(f"RapidAPIScraper: parsed {len(posts)} posts in attempt {attempt}. Total posts accumulated: {len(all_posts)}")
            
            # Parse next cursor dynamically using robust find_cursor_in_dict
            next_c = None
            if isinstance(result, dict):
                next_c = find_cursor_in_dict(result)
            if not next_c and isinstance(res_json, dict):
                next_c = find_cursor_in_dict(res_json)
                
            logger.info(f"RapidAPIScraper: extracted next cursor: '{next_c}'")
            
            current_cursor = next_c
            
            # If no next cursor, or we have already collected at least 100 posts, stop fetching
            if not current_cursor:
                logger.info("RapidAPIScraper: breaking loop because next cursor is None/empty.")
                break
            if len(all_posts) >= 100:
                logger.info(f"RapidAPIScraper: breaking loop because accumulated posts count ({len(all_posts)}) is >= 100.")
                break
                
        unique_all_posts = []
        seen_keys = set()
        for p in all_posts:
            k = (p.get("id"), p.get("media_url"))
            if k not in seen_keys:
                seen_keys.add(k)
                unique_all_posts.append(p)
        return unique_all_posts, current_cursor


class ApifyScraper:
    @staticmethod
    def get_user_info(username, api_key, host=None):
        username_clean = username.lstrip("@").strip()
        url = f"https://api.apify.com/v2/actors/apify~instagram-scraper/run-sync-get-dataset-items?token={api_key}"
        payload = {
            "directUrls": [f"https://www.instagram.com/{username_clean}/"],
            "resultsType": "details",
            "resultsLimit": 1
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url, json=payload, headers=headers, timeout=120)
        if response.status_code not in [200, 201]:
            raise RuntimeError(f"Apify request failed with status {response.status_code}: {response.text}")
        
        items = response.json()
        if not items or not isinstance(items, list):
            raise RuntimeError(f"Apify profile scraper returned no results or invalid format for {username_clean}")
        
        item = items[0]
        if not isinstance(item, dict):
            raise RuntimeError(f"Invalid item type from Apify: {type(item)}")
            
        followers = int(item.get("followersCount") or 0)
        following = int(item.get("followsCount") or item.get("followingCount") or 0)
        posts_count = int(item.get("postsCount") or item.get("mediaCount") or 0)
        
        return {
            "username": item.get("username") or username_clean,
            "full_name": item.get("fullName") or "",
            "biography": item.get("biography") or "",
            "profile_pic_url": item.get("profilePicUrl") or "",
            "followers_count": followers,
            "following_count": following,
            "posts_count": posts_count
        }

    @staticmethod
    def get_latest_posts(username, api_key, host=None, cursor=None):
        username_clean = username.lstrip("@").strip()
        url = f"https://api.apify.com/v2/actors/apify~instagram-scraper/run-sync-get-dataset-items?token={api_key}"
        payload = {
            "directUrls": [f"https://www.instagram.com/{username_clean}/"],
            "resultsType": "posts",
            "resultsLimit": 100
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url, json=payload, headers=headers, timeout=300)
        if response.status_code not in [200, 201]:
            raise RuntimeError(f"Apify request failed with status {response.status_code}: {response.text}")
        
        items = response.json()
        if not items or not isinstance(items, list):
            return [], None
            
        posts = []
        for item in items:
            if not isinstance(item, dict):
                continue
                
            post_id = item.get("id") or item.get("shortCode")
            if not post_id:
                continue
                
            taken_at = int(time.time())
            ts_str = item.get("timestamp")
            if ts_str:
                try:
                    if ts_str.endswith("Z"):
                        ts_str = ts_str.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(ts_str)
                    taken_at = int(dt.timestamp())
                except:
                    pass
            
            caption = item.get("caption") or ""
            likes = int(item.get("likesCount") or 0)
            comments = int(item.get("commentsCount") or 0)
            
            item_type = str(item.get("type", "")).lower()
            is_video = "video" in item_type or item.get("videoUrl") is not None
            
            carousel_children = item.get("carouselChildren")
            if isinstance(carousel_children, list) and len(carousel_children) > 1 and not is_video:
                for child in carousel_children:
                    child_is_video = "video" in str(child.get("type", "")).lower() or child.get("videoUrl") is not None
                    child_media_type = "video" if child_is_video else "image"
                    child_url = child.get("videoUrl") if child_is_video else child.get("url")
                    if not child_url:
                        child_url = child.get("url") or child.get("displayUrl")
                    if child_url:
                        posts.append({
                            "id": str(post_id),
                            "media_url": child_url,
                            "media_type": child_media_type,
                            "caption": caption,
                            "taken_at": taken_at,
                            "likes_count": likes,
                            "comments_count": comments
                        })
            else:
                media_type = "video" if is_video else "image"
                media_url = item.get("videoUrl") if is_video else item.get("displayUrl")
                if not media_url:
                    media_url = item.get("displayUrl") or item.get("url") or ""
                if media_url:
                    posts.append({
                        "id": str(post_id),
                        "media_url": media_url,
                        "media_type": media_type,
                        "caption": caption,
                        "taken_at": taken_at,
                        "likes_count": likes,
                        "comments_count": comments
                    })
        return posts, None

    @staticmethod
    def get_latest_stories(username, api_key, host=None):
        username_clean = username.lstrip("@").strip()
        url = f"https://api.apify.com/v2/actors/apify~instagram-scraper/run-sync-get-dataset-items?token={api_key}"
        
        payload = {
            "directUrls": [f"https://www.instagram.com/{username_clean}/"],
            "resultsType": "stories",
            "resultsLimit": 20
        }
        
        # Load sessionCookie if stored in settings
        cookie = get_instagram_setting("apify_session_cookie", "").strip()
        if cookie:
            payload["sessionCookie"] = cookie
            
        headers = {
            "Content-Type": "application/json"
        }
        
        logger.info(f"ApifyScraper: requesting stories for @{username_clean} (cookie configured: {bool(cookie)})")
        
        response = requests.post(url, json=payload, headers=headers, timeout=120)
        if response.status_code not in [200, 201]:
            logger.error(f"ApifyScraper: stories request failed with status {response.status_code}")
            set_instagram_setting("last_stories_debug", f"Apify: Request failed with status {response.status_code}")
            return []
        
        items = response.json()
        if not items:
            set_instagram_setting("last_stories_debug", "Apify: Response was empty")
            return []
        elif not isinstance(items, list):
            set_instagram_setting("last_stories_debug", f"Apify: Response not list: {str(items)[:150]}")
            return []
        else:
            sample = items[0] if len(items) > 0 else {}
            sample_keys = list(sample.keys()) if isinstance(sample, dict) else []
            set_instagram_setting("last_stories_debug", f"Apify: Fetched {len(items)} items. Sample keys: {sample_keys}")
            
        stories = []
        for item in items:
            if not isinstance(item, dict):
                continue
                
            # Filter out non-story items (e.g. user details or profile metadata)
            item_type = str(item.get("type", "")).lower()
            if item_type in ["user", "profile", "comment", "post"]:
                logger.info(f"ApifyScraper: skipping non-story item of type '{item_type}'")
                continue
            if "biography" in item or "followersCount" in item or "followsCount" in item:
                logger.info("ApifyScraper: skipping user profile object")
                continue
                
            # Filter out permanent story highlights
            if item.get("isHighlight") or item.get("highlightId") or item.get("highlightTitle"):
                logger.info("ApifyScraper: skipping highlight story")
                continue
                
            # Filter out post objects (if scraper fell back or returned posts)
            if "shortcode" in item or "likesCount" in item or "commentsCount" in item:
                logger.info("ApifyScraper: skipping post item (has post fields)")
                continue
                
            story_id = item.get("id") or item.get("storyId") or item.get("pk")
            taken_at = item.get("taken_at") or item.get("taken_at_timestamp") or int(time.time())
            
            ts_str = item.get("timestamp") or item.get("postedAt") or item.get("taken_at")
            if ts_str and isinstance(ts_str, str):
                try:
                    if ts_str.endswith("Z"):
                        ts_str = ts_str.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(ts_str)
                    taken_at = int(dt.timestamp())
                except:
                    pass
                    
            is_video = item.get("videoUrl") is not None or str(item.get("mediaType", "")).lower() == "video"
            media_url = item.get("videoUrl") or item.get("mediaUrl") or item.get("displayUrl") or item.get("url") or ""
            
            if story_id and media_url:
                stories.append({
                    "id": str(story_id),
                    "media_url": media_url,
                    "media_type": "video" if is_video else "image",
                    "taken_at": taken_at
                })
        logger.info(f"ApifyScraper: successfully parsed {len(stories)} stories for @{username_clean}")
        return stories


# --- DYNAMIC KEY ROTATION EXECUTOR WRAPPER ---
def execute_with_key_rotation(method_name, username, cursor=None):
    # Fetch all active keys sorted by requests_count ASC for load balancing
    keys = db_client.execute_query(
        "SELECT id, api_key, provider, host FROM instagram_api_keys WHERE active = 1 ORDER BY requests_count ASC",
        fetch="all"
    )
    
    if not keys:
        # Fallback to MockScraper if no API keys are registered
        scraper_cls = MockScraper
        api_method = getattr(scraper_cls, method_name)
        if method_name == "get_latest_posts":
            return api_method(username, cursor=cursor)
        return api_method(username)
        
    last_err = None
    for kid, api_key, provider, host in keys:
        try:
            logger.info(f"Attempting {method_name} for @{username} using Key ID {kid} ({host})")
            
            if provider == "apify" or host == "apify" or "apify" in str(host).lower() or "apify" in str(provider).lower():
                scraper_cls = ApifyScraper
            elif provider == "hikerapi" or "hikerapi" in str(host).lower() or "hikerapi" in str(provider).lower():
                scraper_cls = HikerAPIScraper
            else:
                scraper_cls = RapidAPIScraper
                
            api_method = getattr(scraper_cls, method_name)
            if method_name == "get_latest_posts":
                result = api_method(username, api_key, host, cursor=cursor)
            else:
                result = api_method(username, api_key, host)
            
            # Request Success! Increment database counter
            db_client.execute_query(
                "UPDATE instagram_api_keys SET requests_count = requests_count + 1 WHERE id = %s",
                (kid,), commit=True
            )
            return result
        except Exception as e:
            err_str = str(e)
            last_err = e
            logger.error(f"Request failed with Key ID {kid}: {err_str}")
            
            # Detect Rate Limits or Quota exhaustion
            is_exhausted = False
            if any(x in err_str or x in err_str.lower() for x in ["429", "403", "402", "limit", "exceeded", "forbidden", "payment"]):
                is_exhausted = True
                
            if is_exhausted:
                db_client.execute_query(
                    "UPDATE instagram_api_keys SET active = 0 WHERE id = %s",
                    (kid,), commit=True
                )
                logger.warning(f"Key ID {kid} marked INACTIVE (Exhausted / Rate Limited).")
                
                # Send alert notification to admin
                chat_id = get_instagram_setting("notification_chat_id", "")
                if chat_id:
                    try:
                        masked_key = api_key[:6] + "..." + api_key[-4:] if len(api_key) > 10 else "..."
                        bot.send_message(
                            chat_id,
                            f"⚠️ <b>Insta API Key Alert</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"Key ID {kid}: <code>{masked_key}</code> ({host}) has been marked as <b>Exhausted / Rate Limited</b> (Error: {err_str}).\n"
                            f"The bot is automatically rotating to the next available key.",
                            parse_mode="HTML"
                        )
                    except Exception as tg_err:
                        logger.error(f"Failed to send admin notification: {tg_err}")
            
            # Continue trying remaining keys in the rotation
            continue
            
    if last_err:
        err_str = str(last_err)
        if "404" in err_str:
            raise Exception(f"Username @{username} not found on Instagram (Status 404).")
        if "400" in err_str:
            raise Exception(f"Invalid username @{username} (Status 400).")
            
    raise Exception(f"All active API keys failed. Last error: {last_err}")


# --- MOCK IMAGES & VIDEOS ---
MOCK_IMAGES = [
    "https://images.unsplash.com/photo-1506744038136-46273834b3fb?w=800&auto=format&fit=crop", # Mountain
    "https://images.unsplash.com/photo-1517841905240-472988babdf9?w=800&auto=format&fit=crop", # Dog
    "https://images.unsplash.com/photo-1498050108023-c5249f4df085?w=800&auto=format&fit=crop"  # Desk
]

MOCK_VIDEOS = [
    "https://assets.mixkit.co/videos/preview/mixkit-forest-stream-in-the-sunlight-529-large.mp4"
]


# --- INLINE KEYBOARD MARKUP BUILDERS ---
def get_instagram_menu_markup():
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup()
    
    # Retrieve tracked targets
    targets = db_client.execute_query("SELECT id, username FROM instagram_targets ORDER BY username ASC", fetch="all")
    if targets:
        for tid, username in targets:
            markup.row(InlineKeyboardButton(f"👤 @{username}", callback_data=f"instagram_view_target:{tid}"))
            
    # Check if there is history
    history_count = db_client.execute_query("SELECT count(*) FROM instagram_search_profiles", fetch="one")
    has_history = history_count and history_count[0] > 0
    
    if has_history:
        markup.row(
            InlineKeyboardButton("➕ Add Target ID", callback_data="instagram_add_target"),
            InlineKeyboardButton("🔍 Instant Search", callback_data="instagram_search_prompt")
        )
        markup.row(
            InlineKeyboardButton("📜 History", callback_data="instagram_history")
        )
    else:
        markup.row(
            InlineKeyboardButton("➕ Add Target ID", callback_data="instagram_add_target"),
            InlineKeyboardButton("🔍 Instant Search", callback_data="instagram_search_prompt")
        )
    markup.row(InlineKeyboardButton("🔑 Insta API", callback_data="instagram_api_menu"))
    
    if not is_standalone:
        markup.row(InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="instagram_back_to_admin"))
    else:
        markup.row(InlineKeyboardButton("❌ Close Panel", callback_data="instagram_close"))
        
    return markup


def get_instagram_profile_markup(target_id, ia_stories_active, ia_interval, ia_posts_active, ia_posts_interval):
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup()
    
    ia_stories_status = f"🟢 IA Stories: ON ({ia_interval}m)" if ia_stories_active else "🔴 IA Stories: OFF"
    ia_posts_status = f"🟢 IA Posts: ON ({ia_posts_interval}m)" if ia_posts_active else "🔴 IA Posts: OFF"
    
    # Row 1: Manual Action buttons
    markup.row(
        InlineKeyboardButton("⚡ Check Stories", callback_data=f"instagram_stories:{target_id}"),
        InlineKeyboardButton("🔄 Refresh Profile", callback_data=f"instagram_refresh:{target_id}")
    )
    # Row 2: Per-profile Automation configs
    markup.row(
        InlineKeyboardButton(ia_stories_status, callback_data=f"instagram_ia_config_menu:{target_id}"),
        InlineKeyboardButton(ia_posts_status, callback_data=f"instagram_ia_posts_config_menu:{target_id}")
    )
    # Row 3: Management & Navigation
    markup.row(
        InlineKeyboardButton("🗑️ Delete Target", callback_data=f"instagram_delete_confirm:{target_id}"),
        InlineKeyboardButton("⬅️ Back to List", callback_data="instagram_main_menu")
    )
    return markup


def get_instagram_profile_ia_markup(target_id, ia_stories_active):
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup()
    
    markup.row(
        InlineKeyboardButton("5 Min", callback_data=f"instagram_ia_set:{target_id}:5"),
        InlineKeyboardButton("10 Min", callback_data=f"instagram_ia_set:{target_id}:10")
    )
    markup.row(
        InlineKeyboardButton("15 Min", callback_data=f"instagram_ia_set:{target_id}:15"),
        InlineKeyboardButton("30 Min", callback_data=f"instagram_ia_set:{target_id}:30")
    )
    markup.row(
        InlineKeyboardButton("1 Hour", callback_data=f"instagram_ia_set:{target_id}:60"),
        InlineKeyboardButton("2 Hours", callback_data=f"instagram_ia_set:{target_id}:120")
    )
    
    if ia_stories_active:
        markup.row(InlineKeyboardButton("🔴 Turn OFF Automatic System", callback_data=f"instagram_ia_off:{target_id}"))
        
    markup.row(
        InlineKeyboardButton("👥 Delivery Receivers", callback_data=f"instagram_ia_receivers_menu:{target_id}:stories"),
        InlineKeyboardButton("📢 Set Alert Chat ID", callback_data=f"instagram_ia_chat:{target_id}")
    )
    markup.row(
        InlineKeyboardButton("⬅️ Back to Profile", callback_data=f"instagram_view_target:{target_id}")
    )
    return markup


def get_instagram_profile_ia_posts_markup(target_id, ia_posts_active):
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup()
    
    markup.row(
        InlineKeyboardButton("5 Min", callback_data=f"instagram_ia_posts_set:{target_id}:5"),
        InlineKeyboardButton("10 Min", callback_data=f"instagram_ia_posts_set:{target_id}:10")
    )
    markup.row(
        InlineKeyboardButton("15 Min", callback_data=f"instagram_ia_posts_set:{target_id}:15"),
        InlineKeyboardButton("30 Min", callback_data=f"instagram_ia_posts_set:{target_id}:30")
    )
    markup.row(
        InlineKeyboardButton("1 Hour", callback_data=f"instagram_ia_posts_set:{target_id}:60"),
        InlineKeyboardButton("2 Hours", callback_data=f"instagram_ia_posts_set:{target_id}:120")
    )
    
    if ia_posts_active:
        markup.row(InlineKeyboardButton("🔴 Turn OFF Automatic Posts System", callback_data=f"instagram_ia_posts_off:{target_id}"))
        
    markup.row(
        InlineKeyboardButton("👥 Delivery Receivers", callback_data=f"instagram_ia_receivers_menu:{target_id}:posts"),
        InlineKeyboardButton("📢 Set Alert Chat ID", callback_data=f"instagram_ia_posts_chat:{target_id}")
    )
    markup.row(
        InlineKeyboardButton("⬅️ Back to Profile", callback_data=f"instagram_view_target:{target_id}")
    )
    return markup


def get_instagram_receivers_markup(target_id, type_prefix, managers_list, userbots_list, active_map):
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup()
    
    # Render Managers (Admins)
    if managers_list:
        for mid, mname in managers_list:
            is_active = active_map.get(str(mid), 0) == 1
            status_bullet = "✅" if is_active else "⬜"
            markup.row(InlineKeyboardButton(
                f"{status_bullet} {mname}",
                callback_data=f"instagram_ia_receiver_toggle:{target_id}:{type_prefix}:manager:{mid}"
            ))
            
    # Render Userbots
    if userbots_list:
        for uid, uname, phone in userbots_list:
            is_active = active_map.get(str(uid), 0) == 1
            status_bullet = "✅" if is_active else "⬜"
            display_name = f"@{uname}" if uname else f"+{phone}"
            markup.row(InlineKeyboardButton(
                f"{status_bullet} Userbot: {display_name}",
                callback_data=f"instagram_ia_receiver_toggle:{target_id}:{type_prefix}:userbot:{uid}"
            ))
            
    parent_cb = f"instagram_ia_config_menu:{target_id}" if type_prefix == "stories" else f"instagram_ia_posts_config_menu:{target_id}"
    markup.row(InlineKeyboardButton("⬅️ Back", callback_data=parent_cb))
    return markup


def get_instagram_confirm_delete_markup(target_id):
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("✅ Yes, Delete", callback_data=f"instagram_delete_yes:{target_id}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"instagram_view_target:{target_id}")
    )
    return markup


def get_instagram_api_keys_markup():
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup()
    
    markup.row(
        InlineKeyboardButton("➕ Add RapidAPI Key", callback_data="instagram_api_add_rapidapi"),
        InlineKeyboardButton("➕ Add Apify Key", callback_data="instagram_api_add_apify")
    )
    markup.row(
        InlineKeyboardButton("➕ Add HikerAPI Key", callback_data="instagram_api_add_hikerapi")
    )
    markup.row(
        InlineKeyboardButton("🍪 Set Apify Session Cookie", callback_data="instagram_api_add_apify_cookie"),
        InlineKeyboardButton("🗑️ Clear Apify Cookie", callback_data="instagram_api_clear_apify_cookie")
    )
    markup.row(
        InlineKeyboardButton("🔄 Reset Exhausted Keys", callback_data="instagram_api_reset"),
        InlineKeyboardButton("🗑️ Delete API Key", callback_data="instagram_api_delete_select")
    )
    markup.row(
        InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="instagram_main_menu")
    )
    return markup


# --- TELEGRAM HANDLERS (Business Logic) ---
def handle_instagram_callbacks(bot_instance, call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    data = call.data
    
    # Verify Admin Access (integrated mode only)
    if not is_standalone:
        is_user_admin = True  # Default to True inside the admin-only panel if check cannot resolve
        found_check_fn = False
        if main_module and hasattr(main_module, "is_admin"):
            try:
                is_user_admin = getattr(main_module, "is_admin")(user_id)
                found_check_fn = True
            except Exception as e:
                logger.error(f"Error checking is_admin on main_module: {e}")
        else:
            for mod_name in ["bot", "userbot", "userbot_v2", "userbot_v3", "main"]:
                try:
                    mod = sys.modules.get(mod_name)
                    if mod and hasattr(mod, "is_admin"):
                        is_user_admin = getattr(mod, "is_admin")(user_id)
                        found_check_fn = True
                        break
                except Exception:
                    pass
        if found_check_fn and not is_user_admin:
            bot_instance.answer_callback_query(call.id, "❌ Access Denied: Admin only.", show_alert=True)
            return

    if data == "instagram_main_menu":
        bot_instance.answer_callback_query(call.id)
        menu_text = (
            "📸 <b>INSTAGRAM TRACKING PANEL</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Monitor public Instagram accounts and fetch stories in real-time.\n\n"
            "💡 <i>Tapping a user profile displays details from the database instantly without calling the API.</i>"
        )
        try:
            bot_instance.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=menu_text,
                reply_markup=get_instagram_menu_markup(),
                parse_mode="HTML"
            )
        except Exception:
            try:
                bot_instance.delete_message(chat_id, call.message.message_id)
            except:
                pass
            bot_instance.send_message(
                chat_id,
                menu_text,
                reply_markup=get_instagram_menu_markup(),
                parse_mode="HTML"
            )
            
    elif data == "instagram_add_target":
        bot_instance.answer_callback_query(call.id)
        user_states[user_id] = "WAITING_FOR_INSTAGRAM_ID"
            
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "📸 <b>Add Instagram Target</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Please send the Instagram Username/ID you want to track (e.g. <code>cristiano</code>):\n\n"
            "Type <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )

    elif data == "instagram_search_prompt":
        bot_instance.answer_callback_query(call.id)
        user_states[user_id] = "WAITING_FOR_INSTAGRAM_SEARCH"
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "🔍 <b>Instant Account Search</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Please send the Instagram Username/ID you want to inspect (e.g. <code>cristiano</code>):\n\n"
            "Type <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )

    elif data.startswith("instagram_view_target:"):
        bot_instance.answer_callback_query(call.id)
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query(
            "SELECT id, username, full_name, biography, profile_pic_url, followers_count, following_count, posts_count, last_refreshed_at, "
            "ia_stories, ia_interval, ia_posts, ia_post_interval "
            "FROM instagram_targets WHERE id = %s", (target_id,), fetch="one"
        )
        if not row:
            bot_instance.send_message(chat_id, "❌ Instagram profile not found in database.", parse_mode="HTML")
            return
            
        tid, username, full_name, bio, pic_url, followers, following, posts, last_refreshed, ia_stories, ia_interval, ia_posts, ia_post_interval = row
        ia_stories_active = (ia_stories == 1)
        ia_posts_active = (ia_posts == 1)
        
        if isinstance(last_refreshed, str):
            try:
                dt = datetime.fromisoformat(last_refreshed)
                last_refreshed_str = dt.strftime('%I:%M %p | %b %d, %Y')
            except:
                last_refreshed_str = last_refreshed
        else:
            last_refreshed_str = last_refreshed.strftime('%I:%M %p | %b %d, %Y') if last_refreshed else "Never"

        # Delivered counts (only display if active!)
        delivered_stats = ""
        if ia_stories_active or ia_posts_active:
            chat_val = get_instagram_setting("notification_chat_id", "")
            chat_display = f"<code>{chat_val}</code>" if chat_val else "<i>Not Configured</i>"
            
            story_line = ""
            if ia_stories_active:
                count_row = db_client.execute_query("SELECT count(*) FROM instagram_seen_stories WHERE username = %s", (username.lower(),), fetch="one")
                delivered_count = count_row[0] if count_row else 0
                story_line = f"⚡ <b>IA Stories:</b> 🟢 ACTIVE (every {ia_interval}m)\n📥 <b>Auto Stories Delivered:</b> <code>{delivered_count}</code>\n"
                
            post_line = ""
            if ia_posts_active:
                count_posts_row = db_client.execute_query("SELECT count(*) FROM instagram_seen_posts WHERE username = %s", (username.lower(),), fetch="one")
                delivered_posts_count = count_posts_row[0] if count_posts_row else 0
                post_line = f"⚡ <b>IA Posts:</b> 🟢 ACTIVE (every {ia_post_interval}m)\n📥 <b>Auto Posts Delivered:</b> <code>{delivered_posts_count}</code>\n"
                
            delivered_stats = (
                f"{story_line}"
                f"{post_line}"
                f"📢 <b>Notification Chat:</b> {chat_display}\n\n"
            )

        caption = (
            f"👤 <b>Instagram Profile: @{username}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>Name:</b> {full_name or 'None'}\n"
            f"📝 <b>Bio:</b> <i>{bio or 'None'}</i>\n\n"
            f"👥 <b>Followers:</b> <code>{followers:,}</code>\n"
            f"🔄 <b>Following:</b> <code>{following:,}</code>\n"
            f"📸 <b>Posts:</b> <code>{posts:,}</code>\n\n"
            f"{delivered_stats}"
            f"🕒 <b>Last Refreshed:</b> <i>{last_refreshed_str}</i>"
        )
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        if pic_url:
            lf = download_media_temp(pic_url)
            if lf:
                try:
                    with open(lf, 'rb') as f:
                        bot_instance.send_photo(
                            chat_id,
                            f,
                            caption=caption,
                            reply_markup=get_instagram_profile_markup(target_id, ia_stories_active, ia_interval, ia_posts_active, ia_post_interval),
                            parse_mode="HTML"
                        )
                    return
                except Exception as e:
                    logger.warning(f"Failed to send local profile photo file: {e}")
                finally:
                    try:
                        os.remove(lf)
                    except:
                        pass
            
            # Fallback to direct URL if local download failed or sending failed
            try:
                bot_instance.send_photo(
                    chat_id,
                    pic_url,
                    caption=caption,
                    reply_markup=get_instagram_profile_markup(target_id, ia_stories_active, ia_interval, ia_posts_active, ia_post_interval),
                    parse_mode="HTML"
                )
                return
            except Exception as e:
                logger.warning(f"Failed to send profile photo by URL: {e}. Falling back to text.")
                
        bot_instance.send_message(
            chat_id,
            f"🖼️ <a href='{pic_url or ''}'>Profile Photo</a>\n\n{caption}",
            reply_markup=get_instagram_profile_markup(target_id, ia_stories_active, ia_interval, ia_posts_active, ia_post_interval),
            parse_mode="HTML",
            disable_web_page_preview=True
        )

    # --- PROFILE SPECIFIC IA STORIES CONFIG CALLBACKS ---
    elif data.startswith("instagram_ia_config_menu:"):
        bot_instance.answer_callback_query(call.id)
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query("SELECT username, ia_stories, ia_interval FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.send_message(chat_id, "❌ Instagram profile not found in database.", parse_mode="HTML")
            return
            
        username, ia_stories, ia_interval = row
        ia_stories_active = (ia_stories == 1)
        chat_val = get_instagram_setting("notification_chat_id", "")
        chat_display = f"<code>{chat_val}</code>" if chat_val else "<i>Not Configured</i>"
        
        config_text = (
            f"⚡ <b>IA Stories Configuration: @{username}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Configure automatic story checking and notifications for this specific account.\n\n"
            f"🔘 <b>Automation Status:</b> {'🟢 ACTIVE' if ia_stories_active else '🔴 DISABLED'}\n"
            f"⏱️ <b>Check Interval:</b> <code>{ia_interval}</code> Minutes\n"
            f"📢 <b>Notification Chat:</b> {chat_display}\n\n"
            f"💡 <i>Tapping a polling interval below will immediately activate the automatic fetcher for @{username}.</i>"
        )
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            config_text,
            reply_markup=get_instagram_profile_ia_markup(target_id, ia_stories_active),
            parse_mode="HTML"
        )

    elif data.startswith("instagram_ia_set:"):
        parts = data.split(":")
        target_id = int(parts[1])
        interval = int(parts[2])
        
        row = db_client.execute_query("SELECT username FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.answer_callback_query(call.id, "❌ Profile not found.", show_alert=True)
            return
        username = row[0]
        
        db_client.execute_query(
            "UPDATE instagram_targets SET ia_stories = 1, ia_interval = %s, last_auto_poll_time = '0' WHERE id = %s",
            (interval, target_id), commit=True
        )
        
        bot_instance.answer_callback_query(call.id, f"🟢 IA Stories enabled (check every {interval}m) for @{username}!", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, f"instagram_view_target:{target_id}"))

    elif data.startswith("instagram_ia_off:"):
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query("SELECT username FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.answer_callback_query(call.id, "❌ Profile not found.", show_alert=True)
            return
        username = row[0]
        
        db_client.execute_query("UPDATE instagram_targets SET ia_stories = 0 WHERE id = %s", (target_id,), commit=True)
        bot_instance.answer_callback_query(call.id, f"🔴 IA Stories disabled for @{username}.", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, f"instagram_view_target:{target_id}"))

    elif data.startswith("instagram_ia_chat:"):
        bot_instance.answer_callback_query(call.id)
        target_id = int(data.split(":")[-1])
        
        user_states[user_id] = f"WAITING_FOR_INSTAGRAM_CHAT_ID:{target_id}"
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "📢 <b>Configure Notification Chat</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Please send the Telegram Chat ID or Channel Username (e.g. <code>-100123456789</code> or <code>@my_channel</code>) "
            "where automated story notifications will be delivered.\n\n"
            f"💡 <i>Tip: Send <code>/current</code> to use this current chat (ID: <code>{chat_id}</code>) or send <code>/cancel</code> to abort.</i>",
            parse_mode="HTML"
        )

    # --- PROFILE SPECIFIC IA POSTS CONFIG CALLBACKS ---
    elif data.startswith("instagram_ia_posts_config_menu:"):
        bot_instance.answer_callback_query(call.id)
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query("SELECT username, ia_posts, ia_post_interval FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.send_message(chat_id, "❌ Instagram profile not found in database.", parse_mode="HTML")
            return
            
        username, ia_posts, ia_post_interval = row
        ia_posts_active = (ia_posts == 1)
        chat_val = get_instagram_setting("notification_chat_id", "")
        chat_display = f"<code>{chat_val}</code>" if chat_val else "<i>Not Configured</i>"
        
        config_text = (
            f"⚡ <b>IA Posts Configuration: @{username}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Configure automatic post checking and notifications for this specific account.\n\n"
            f"🔘 <b>Automation Status:</b> {'🟢 ACTIVE' if ia_posts_active else '🔴 DISABLED'}\n"
            f"⏱️ <b>Check Interval:</b> <code>{ia_post_interval}</code> Minutes\n"
            f"📢 <b>Notification Chat:</b> {chat_display}\n\n"
            f"💡 <i>Tapping a polling interval below will immediately activate the automatic post fetcher for @{username}.</i>"
        )
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            config_text,
            reply_markup=get_instagram_profile_ia_posts_markup(target_id, ia_posts_active),
            parse_mode="HTML"
        )

    elif data.startswith("instagram_ia_posts_set:"):
        parts = data.split(":")
        target_id = int(parts[1])
        interval = int(parts[2])
        
        row = db_client.execute_query("SELECT username FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.answer_callback_query(call.id, "❌ Profile not found.", show_alert=True)
            return
        username = row[0]
        
        db_client.execute_query(
            "UPDATE instagram_targets SET ia_posts = 1, ia_post_interval = %s, last_auto_post_poll_time = '0' WHERE id = %s",
            (interval, target_id), commit=True
        )
        
        bot_instance.answer_callback_query(call.id, f"🟢 IA Posts enabled (check every {interval}m) for @{username}!", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, f"instagram_view_target:{target_id}"))

    elif data.startswith("instagram_ia_posts_off:"):
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query("SELECT username FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.answer_callback_query(call.id, "❌ Profile not found.", show_alert=True)
            return
        username = row[0]
        
        db_client.execute_query("UPDATE instagram_targets SET ia_posts = 0 WHERE id = %s", (target_id,), commit=True)
        bot_instance.answer_callback_query(call.id, f"🔴 IA Posts disabled for @{username}.", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, f"instagram_view_target:{target_id}"))

    elif data.startswith("instagram_ia_posts_chat:"):
        bot_instance.answer_callback_query(call.id)
        target_id = int(data.split(":")[-1])
        
        user_states[user_id] = f"WAITING_FOR_INSTAGRAM_CHAT_ID:{target_id}"
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "📢 <b>Configure Notification Chat</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Please send the Telegram Chat ID or Channel Username (e.g. <code>-100123456789</code> or <code>@my_channel</code>) "
            "where automated post notifications will be delivered.\n\n"
            f"💡 <i>Tip: Send <code>/current</code> to use this current chat (ID: <code>{chat_id}</code>) or send <code>/cancel</code> to abort.</i>",
            parse_mode="HTML"
        )

    # --- IA DELIVERY RECEIVERS CHOOSE CALLBACKS ---
    elif data.startswith("instagram_ia_receivers_menu:"):
        bot_instance.answer_callback_query(call.id)
        parts = data.split(":")
        target_id = int(parts[1])
        type_prefix = parts[2] # 'stories' or 'posts'
        
        # Load Managers (Admins)
        managers_list = []
        if not is_standalone:
            try:
                admin_list = getattr(main_module, 'config', {}).get("admin_ids", [])
                for idx, adm in enumerate(admin_list):
                    managers_list.append((int(adm), f"Admin {idx+1}"))
            except:
                pass
        # Fallback manager if empty
        if not managers_list:
            managers_list.append((user_id, "Manager (You)"))
            
        # Load linked Userbots
        userbots_list = []
        try:
            if db_client.table_exists("userbot_sessions"):
                rows = db_client.execute_query(
                    "SELECT user_id, username, phone FROM userbot_sessions WHERE session_string IS NOT NULL AND session_string != ''",
                    fetch="all"
                )
                if rows:
                    for uid, uname, phone in rows:
                        if uid:
                            userbots_list.append((uid, uname, phone))
            elif db_client.table_exists("linked_userbots"):
                rows = db_client.execute_query(
                    "SELECT user_id, username, phone FROM linked_userbots WHERE session_string IS NOT NULL AND session_string != ''",
                    fetch="all"
                )
                if rows:
                    for uid, uname, phone in rows:
                        if uid:
                            userbots_list.append((uid, uname, phone))
        except Exception as e:
            logger.error(f"Error loading userbots for menu: {e}")
            
        # Load Active Map
        active_map = {}
        try:
            rows = db_client.execute_query(
                "SELECT telegram_id, stories_enabled, posts_enabled FROM instagram_target_receivers WHERE target_id = %s",
                (target_id,), fetch="all"
            )
            if rows:
                for tg_id, s_ok, p_ok in rows:
                    val = s_ok if type_prefix == "stories" else p_ok
                    active_map[str(tg_id)] = val
        except:
            pass
            
        menu_text = (
            f"👥 <b>Select Delivery Receivers ({type_prefix.upper()})</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Choose which linked userbots and managers should receive automated {type_prefix} alerts:\n\n"
            f"💡 <i>Tapping a destination toggles its inclusion. Active destinations are marked with ✅.</i>"
        )
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            menu_text,
            reply_markup=get_instagram_receivers_markup(target_id, type_prefix, managers_list, userbots_list, active_map),
            parse_mode="HTML"
        )

    elif data.startswith("instagram_ia_receiver_toggle:"):
        parts = data.split(":")
        target_id = int(parts[1])
        type_prefix = parts[2]
        rec_type = parts[3]
        telegram_id = str(parts[4])
        
        # Check active receivers
        row = db_client.execute_query(
            "SELECT stories_enabled, posts_enabled FROM instagram_target_receivers WHERE target_id = %s AND telegram_id = %s",
            (target_id, telegram_id), fetch="one"
        )
        
        stories_ok = 0
        posts_ok = 0
        
        if row:
            stories_ok, posts_ok = row
            if type_prefix == "stories":
                stories_ok = 0 if stories_ok == 1 else 1
            else:
                posts_ok = 0 if posts_ok == 1 else 1
                
            db_client.execute_query(
                "UPDATE instagram_target_receivers SET stories_enabled = %s, posts_enabled = %s WHERE target_id = %s AND telegram_id = %s",
                (stories_ok, posts_ok, target_id, telegram_id), commit=True
            )
        else:
            name = f"{rec_type.title()} {telegram_id}"
            if type_prefix == "stories":
                stories_ok = 1
            else:
                posts_ok = 1
                
            db_client.execute_query(
                "INSERT INTO instagram_target_receivers (target_id, telegram_id, name, receiver_type, stories_enabled, posts_enabled) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (target_id, telegram_id, name, rec_type, stories_ok, posts_ok), commit=True
            )
            
        bot_instance.answer_callback_query(call.id, "🔄 Receiver state updated!")
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, f"instagram_ia_receivers_menu:{target_id}:{type_prefix}"))

    # --- MANUAL STORIES CHECK ---
    elif data.startswith("instagram_stories:"):
        bot_instance.answer_callback_query(call.id, "⏳ Fetching stories...")
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query("SELECT username FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.send_message(chat_id, "❌ Instagram profile not found in database.", parse_mode="HTML")
            return
        username = row[0]
        
        status_msg = bot_instance.send_message(chat_id, f"⏳ <b>Checking stories for @{username}...</b>", parse_mode="HTML")
        
        try:
            stories = execute_with_key_rotation("get_latest_stories", username)
            
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
                
            if not stories:
                debug_info = get_instagram_setting("last_stories_debug", "No debug info recorded")
                bot_instance.send_message(
                    chat_id, 
                    f"ℹ️ <b>No active stories</b> found for @{username} in the last 24h.\n\n"
                    f"🔬 <b>Debug Details:</b>\n<code>{debug_info}</code>", 
                    parse_mode="HTML"
                )
                return
                
            bot_instance.send_message(chat_id, f"⚡ <b>Found {len(stories)} active stories for @{username}:</b>", parse_mode="HTML")
            
            for idx, story in enumerate(stories):
                s_url = story["media_url"]
                s_type = story["media_type"]
                taken_ts = story["taken_at"]
                taken_str = datetime.fromtimestamp(taken_ts).strftime('%I:%M %p | %b %d, %Y')
                
                caption = f"⚡ <b>Story {idx+1}/{len(stories)} by @{username}</b>\n🕒 Uploaded: <i>{taken_str}</i>"
                
                sent = False
                lf = download_media_temp(s_url)
                if lf:
                    try:
                        with open(lf, 'rb') as f:
                            if s_type == "video":
                                bot_instance.send_video(chat_id, f, caption=caption, parse_mode="HTML")
                            else:
                                bot_instance.send_photo(chat_id, f, caption=caption, parse_mode="HTML")
                        sent = True
                    except Exception as e:
                        logger.warning(f"Failed to send local story file: {e}")
                    finally:
                        try:
                            os.remove(lf)
                        except:
                            pass
                
                if not sent:
                    try:
                        if s_type == "video":
                            bot_instance.send_video(chat_id, s_url, caption=caption, parse_mode="HTML")
                        else:
                            bot_instance.send_photo(chat_id, s_url, caption=caption, parse_mode="HTML")
                    except Exception as e:
                        logger.warning(f"Story Direct upload failed: {e}. Sending fallback link.")
                        bot_instance.send_message(
                            chat_id,
                            f"⚡ <b>Story {idx+1}/{len(stories)} by @{username}</b>\n"
                            f"🕒 Uploaded: <i>{taken_str}</i>\n\n"
                            f"🔗 <a href='{s_url}'>Direct Media Link ({s_type.upper()})</a>",
                            parse_mode="HTML"
                        )
                    
        except Exception as e:
            logger.error(f"Error checking stories: {e}")
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
            bot_instance.send_message(chat_id, f"❌ <b>Error fetching stories:</b> {e}", parse_mode="HTML")

    elif data.startswith("instagram_refresh:"):
        bot_instance.answer_callback_query(call.id, "⏳ Refreshing...")
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query("SELECT username FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.send_message(chat_id, "❌ Instagram profile not found in database.", parse_mode="HTML")
            return
        username = row[0]
        
        status_msg = bot_instance.send_message(chat_id, f"⏳ <b>Refreshing @{username} from Instagram...</b>", parse_mode="HTML")
        
        try:
            profile = execute_with_key_rotation("get_user_info", username)
            
            db_client.execute_query(
                "UPDATE instagram_targets SET full_name = %s, biography = %s, profile_pic_url = %s, "
                "followers_count = %s, following_count = %s, posts_count = %s, last_refreshed_at = %s WHERE id = %s",
                (
                    profile.get("full_name", ""),
                    profile.get("biography", ""),
                    profile.get("profile_pic_url", ""),
                    profile.get("followers_count", 0),
                    profile.get("following_count", 0),
                    profile.get("posts_count", 0),
                    datetime.now().isoformat(),
                    target_id
                ),
                commit=True
            )
            
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
                
            bot_instance.send_message(chat_id, f"✅ <b>Profile refreshed successfully!</b>", parse_mode="HTML")
            
            class CallMock:
                def __init__(self, from_user, message, cid):
                    self.from_user = from_user
                    self.message = message
                    self.data = cid
                    self.id = "0"
            handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, f"instagram_view_target:{target_id}"))
            
        except Exception as e:
            logger.error(f"Error refreshing profile: {e}")
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
            bot_instance.send_message(chat_id, f"❌ <b>Error refreshing profile:</b> {e}", parse_mode="HTML")

    elif data.startswith("instagram_delete_confirm:"):
        bot_instance.answer_callback_query(call.id)
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query("SELECT username FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if not row:
            bot_instance.send_message(chat_id, "❌ Instagram profile not found in database.", parse_mode="HTML")
            return
        username = row[0]
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            f"⚠️ <b>Delete Target ID</b>\n\n"
            f"Are you sure you want to stop tracking and delete @{username} from database?",
            reply_markup=get_instagram_confirm_delete_markup(target_id),
            parse_mode="HTML"
        )

    elif data.startswith("instagram_delete_yes:"):
        target_id = int(data.split(":")[-1])
        
        row = db_client.execute_query("SELECT username FROM instagram_targets WHERE id = %s", (target_id,), fetch="one")
        if row:
            username = row[0]
            db_client.execute_query("DELETE FROM instagram_targets WHERE id = %s", (target_id,), commit=True)
            bot_instance.answer_callback_query(call.id, f"🗑️ Deleted @{username}", show_alert=True)
        else:
            bot_instance.answer_callback_query(call.id, "Target already deleted.")
            
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "📸 <b>INSTAGRAM TRACKING PANEL</b>\n━━━━━━━━━━━━━━━━━━━━\nSelect an Instagram ID to track:",
            reply_markup=get_instagram_menu_markup(),
            parse_mode="HTML"
        )

    elif data == "instagram_back_to_admin":
        bot_instance.answer_callback_query(call.id)
        try:
            from bot import get_admin_panel_markup
            admin_msg = (
                "👑 <b>ADMIN CONTROL PANEL</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "Manage how your bot is displayed to users.\n\n"
                "💡 Select a module below to configure settings:"
            )
            bot_instance.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=admin_msg,
                reply_markup=get_admin_panel_markup(),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Error returning to main admin menu: {e}")

    elif data == "instagram_history":
        bot_instance.answer_callback_query(call.id)
        profiles = db_client.execute_query("SELECT username FROM instagram_search_profiles ORDER BY last_searched_at DESC", fetch="all")
        
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        markup = InlineKeyboardMarkup()
        if profiles:
            for p in profiles:
                uname = p[0]
                markup.row(InlineKeyboardButton(f"👤 @{uname}", callback_data=f"instagram_history_view:{uname}"))
                
        markup.row(
            InlineKeyboardButton("🧹 Clear All History", callback_data="instagram_history_clear_all"),
            InlineKeyboardButton("⬅️ Main Menu", callback_data="instagram_main_menu")
        )
        
        menu_text = (
            "📜 <b>SEARCH HISTORY PROFILE LIST</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Select a cached profile below to view its details or show its fetched posts offline:\n\n"
            "💡 <i>Cached data consumes 0 API requests when viewing.</i>"
        )
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
        bot_instance.send_message(chat_id, menu_text, reply_markup=markup, parse_mode="HTML")

    elif data.startswith("instagram_history_view:"):
        bot_instance.answer_callback_query(call.id)
        username = data.split(":")[-1].lower()
        
        row = db_client.execute_query(
            "SELECT username, full_name, biography, profile_pic_url, followers_count, following_count, posts_count, last_searched_at "
            "FROM instagram_search_profiles WHERE username = %s", (username,), fetch="one"
        )
        if not row:
            bot_instance.send_message(chat_id, f"❌ Profile history for @{username} not found.", parse_mode="HTML")
            return
            
        uname, full_name, bio, pic_url, followers, following, posts, last_searched = row
        
        if isinstance(last_searched, str):
            last_searched_str = last_searched
        else:
            last_searched_str = last_searched.strftime('%I:%M %p | %b %d, %Y') if last_searched else "Never"
            
        caption = (
            f"🔍 <b>Offline History: @{uname}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>Name:</b> {full_name or 'None'}\n"
            f"📝 <b>Bio:</b> <i>{bio or 'None'}</i>\n\n"
            f"👥 <b>Followers:</b> <code>{followers:,}</code>\n"
            f"🔄 <b>Following:</b> <code>{following:,}</code>\n"
            f"📸 <b>Posts:</b> <code>{posts:,}</code>\n\n"
            f"🕒 <b>Last Searched:</b> <i>{last_searched_str}</i>"
        )
        
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        markup = InlineKeyboardMarkup()
        
        # Get count of currently cached posts
        count_row = db_client.execute_query("SELECT count(*) FROM instagram_search_posts WHERE username = %s", (username,), fetch="one")
        cached_posts = count_row[0] if count_row else 0
        
        markup.row(
            InlineKeyboardButton(f"📸 Show Fetched Posts ({cached_posts})", callback_data=f"instagram_search_page:{username}:0"),
            InlineKeyboardButton("🧹 Clear ID History", callback_data=f"instagram_history_clear_id:{username}")
        )
        markup.row(InlineKeyboardButton("⬅️ Back to History", callback_data="instagram_history"))
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        if pic_url:
            lf = download_media_temp(pic_url)
            if lf:
                try:
                    with open(lf, 'rb') as f:
                        bot_instance.send_photo(chat_id, f, caption=caption, reply_markup=markup, parse_mode="HTML")
                    return
                except:
                    pass
                finally:
                    try:
                        os.remove(lf)
                    except:
                        pass
            try:
                bot_instance.send_photo(chat_id, pic_url, caption=caption, reply_markup=markup, parse_mode="HTML")
                return
            except:
                pass
                
        bot_instance.send_message(chat_id, caption, reply_markup=markup, parse_mode="HTML")

    elif data == "instagram_history_clear_all":
        db_client.execute_query("DELETE FROM instagram_search_profiles", commit=True)
        db_client.execute_query("DELETE FROM instagram_search_posts", commit=True)
        db_client.execute_query("DELETE FROM instagram_settings WHERE key LIKE 'cursor_%'", commit=True)
        
        bot_instance.answer_callback_query(call.id, "🧹 Entire search history cleared!", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, "instagram_main_menu"))

    elif data.startswith("instagram_history_clear_id:"):
        username = data.split(":")[-1].lower()
        db_client.execute_query("DELETE FROM instagram_search_profiles WHERE username = %s", (username,), commit=True)
        db_client.execute_query("DELETE FROM instagram_search_posts WHERE username = %s", (username,), commit=True)
        db_client.execute_query("DELETE FROM instagram_settings WHERE key = %s", (f"cursor_{username}",), commit=True)
        
        bot_instance.answer_callback_query(call.id, f"🧹 Cleared history of @{username}!", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, "instagram_history"))

    elif data.startswith("instagram_fetch_next_batch:"):
        username = data.split(":")[-1].lower()
        cursor = get_instagram_setting(f"cursor_{username}")
        
        if not cursor:
            bot_instance.answer_callback_query(call.id, "⚠️ No more pages available to fetch.", show_alert=True)
            return
            
        bot_instance.answer_callback_query(call.id, "⏳ Fetching next batch from API...")
        status_msg = bot_instance.send_message(chat_id, f"⏳ <b>Contacting Instagram API to fetch more posts for @{username}...</b>", parse_mode="HTML")
        
        try:
            posts, next_c = execute_with_key_rotation("get_latest_posts", username, cursor=cursor)
            
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
                
            if not posts:
                bot_instance.send_message(chat_id, f"ℹ️ No older posts returned by API for @{username}.", parse_mode="HTML")
                db_client.execute_query("DELETE FROM instagram_settings WHERE key = %s", (f"cursor_{username}",), commit=True)
                return
                
            # Get current posts count to calculate new offset
            count_row = db_client.execute_query("SELECT count(*) FROM instagram_search_posts WHERE username = %s", (username,), fetch="one")
            prev_total = count_row[0] if count_row else 0
            
            for post in posts:
                seen = db_client.execute_query("SELECT 1 FROM instagram_search_posts WHERE username = %s AND post_id = %s", (username, post["id"]), fetch="one")
                if not seen:
                    db_client.execute_query(
                        "INSERT INTO instagram_search_posts (username, post_id, media_url, media_type, caption, likes_count, comments_count, taken_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            username,
                            post.get("id", ""),
                            post.get("media_url", ""),
                            post.get("media_type", "image"),
                            post.get("caption", ""),
                            post.get("likes_count", 0),
                            post.get("comments_count", 0),
                            post.get("taken_at", int(time.time()))
                        ),
                        commit=True
                    )
                    
            if next_c:
                set_instagram_setting(f"cursor_{username}", next_c)
            else:
                db_client.execute_query("DELETE FROM instagram_settings WHERE key = %s", (f"cursor_{username}",), commit=True)
                
            try:
                bot_instance.delete_message(chat_id, call.message.message_id)
            except:
                pass
                
            send_search_posts_page(bot_instance, chat_id, username, offset=prev_total)
            
        except Exception as e:
            logger.error(f"Error fetching next batch: {e}")
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
            bot_instance.send_message(chat_id, f"❌ <b>API fetch failed:</b> {e}", parse_mode="HTML")

    elif data == "instagram_close":
        bot_instance.answer_callback_query(call.id)
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
        bot_instance.send_message(chat_id, "🚪 <b>Instagram Scraper Panel closed.</b>", parse_mode="HTML")

    # --- INSTANT SEARCH PAGINATION ---
    elif data.startswith("instagram_search_page:"):
        parts = data.split(":")
        username = parts[1]
        offset = int(parts[2])
        
        bot_instance.answer_callback_query(call.id, f"⏳ Loading next posts...")
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        send_search_posts_page(bot_instance, chat_id, username, offset)

    # --- INSTA API KEYS MANAGER VIEWS ---
    elif data == "instagram_api_menu":
        bot_instance.answer_callback_query(call.id)
        
        keys_rows = db_client.execute_query(
            "SELECT id, api_key, provider, host, requests_count, active FROM instagram_api_keys ORDER BY id ASC",
            fetch="all"
        )
        
        menu_text = (
            "🔑 <b>INSTA API KEYS MANAGER</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Register RapidAPI keys to fetch profile data and search feeds. The bot will automatically cycle through working keys to balance the API requests load.\n\n"
        )
        
        if not keys_rows:
            menu_text += "⚠️ <i>No keys registered yet. The bot is operating in local Mock Scraper mode.</i>"
        else:
            menu_text += "📶 <b>Registered Keys:</b>\n"
            for row in keys_rows:
                kid, key, provider, host, count, active = row
                masked_key = key[:6] + "..." + key[-4:] if len(key) > 10 else "..."
                status_bullet = "🟢 Active" if active == 1 else "🔴 Exhausted"
                menu_text += (
                    f"🔹 <b>ID {kid}</b>: <code>{masked_key}</code> ({provider})\n"
                    f"   ├ <b>Host</b>: {host}\n"
                    f"   ├ <b>Status</b>: {status_bullet}\n"
                    f"   └ <b>Requests Used</b>: <code>{count}</code>\n\n"
                )
                
        # Show active Apify Session Cookie
        cookie = get_instagram_setting("apify_session_cookie", "").strip()
        if cookie:
            masked_cookie = cookie[:8] + "..." + cookie[-8:] if len(cookie) > 16 else "..."
            menu_text += f"🍪 <b>Apify Session Cookie:</b> <code>{masked_cookie}</code>\n\n"
        else:
            menu_text += "🍪 <b>Apify Session Cookie:</b> <i>Not configured (required for Apify stories scraping)</i>\n\n"
            
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            menu_text,
            reply_markup=get_instagram_api_keys_markup(),
            parse_mode="HTML"
        )

    elif data == "instagram_api_add_rapidapi":
        bot_instance.answer_callback_query(call.id)
        user_states[user_id] = "WAITING_FOR_INSTAGRAM_API_KEY"
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "🔑 <b>Add New RapidAPI Key</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Please send your RapidAPI key directly (defaults to <code>instagram-best-experience</code>).\n\n"
            "💡 <i>Advanced users: To use a custom endpoint provider, send the details in the format:</i>\n"
            "<code>KEY:PROVIDER_NAME:API_HOST_HEADER</code>\n"
            "<i>(Example: <code>xyz123abc:rocketapi:instagram-scraper-api2.p.rapidapi.com</code>)</i>\n\n"
            "Send <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )

    elif data == "instagram_api_add_apify":
        bot_instance.answer_callback_query(call.id)
        user_states[user_id] = "WAITING_FOR_INSTAGRAM_APIFY_KEY"
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "🔑 <b>Add New Apify API Key</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Please send your Apify API Token directly (from your Apify Console under Integrations).\n\n"
            "Send <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )

    elif data == "instagram_api_add_hikerapi":
        bot_instance.answer_callback_query(call.id)
        user_states[user_id] = "WAITING_FOR_INSTAGRAM_HIKERAPI_KEY"
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "🔑 <b>Add New HikerAPI Key</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Please send your HikerAPI Access Key directly (from your HikerAPI Dashboard).\n\n"
            "Send <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )

    elif data == "instagram_api_add_apify_cookie":
        bot_instance.answer_callback_query(call.id)
        user_states[user_id] = "WAITING_FOR_INSTAGRAM_APIFY_COOKIE"
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "🍪 <b>Set Apify Session Cookie</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "To scrape Instagram Stories and Highlights using Apify, Instagram requires a logged-in session.\n\n"
            "Please copy your Instagram <code>sessionid</code> cookie from your browser and paste it here.\n"
            "<i>(Example: <code>sessionid=5819203810%3Aabcd...</code>)</i>\n\n"
            "Send <code>/cancel</code> to abort.",
            parse_mode="HTML"
        )

    elif data == "instagram_api_clear_apify_cookie":
        db_client.execute_query("DELETE FROM instagram_settings WHERE key = 'apify_session_cookie'", commit=True)
        bot_instance.answer_callback_query(call.id, "🍪 Apify Session Cookie cleared!", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, "instagram_api_menu"))

    elif data == "instagram_api_reset":
        db_client.execute_query("UPDATE instagram_api_keys SET active = 1", commit=True)
        bot_instance.answer_callback_query(call.id, "🔄 All keys reset back to Active!", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, "instagram_api_menu"))

    elif data == "instagram_api_delete_select":
        bot_instance.answer_callback_query(call.id)
        
        keys_rows = db_client.execute_query("SELECT id, api_key FROM instagram_api_keys ORDER BY id ASC", fetch="all")
        if not keys_rows:
            bot_instance.send_message(chat_id, "ℹ️ No API keys available to delete.")
            return
            
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        markup = InlineKeyboardMarkup()
        for row in keys_rows:
            kid, key = row
            masked_key = key[:6] + "..." + key[-4:] if len(key) > 10 else "..."
            markup.row(InlineKeyboardButton(f"🗑️ Delete ID {kid} ({masked_key})", callback_data=f"instagram_api_delete_confirm:{kid}"))
            
        markup.row(InlineKeyboardButton("⬅️ Cancel", callback_data="instagram_api_menu"))
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
            
        bot_instance.send_message(
            chat_id,
            "🗑️ <b>Delete API Key</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Select which key you want to remove permanently:",
            reply_markup=markup,
            parse_mode="HTML"
        )

    elif data.startswith("instagram_api_delete_confirm:"):
        kid = int(data.split(":")[-1])
        
        db_client.execute_query("DELETE FROM instagram_api_keys WHERE id = %s", (kid,), commit=True)
        bot_instance.answer_callback_query(call.id, f"🗑️ Key ID {kid} deleted successfully.", show_alert=True)
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(call.from_user, call.message, "instagram_api_menu"))


# --- RENDER AND SEND SEARCH POSTS PAGE AS AN ALBUM WITH COMPANION NEXT BUTTON ---
def send_search_posts_page(bot_instance, chat_id, username, offset=0, client_type="bot"):
    # 1. Query exactly 10 parent posts (distinct post_ids ordered chronologically)
    parent_rows = db_client.execute_query(
        "SELECT post_id FROM instagram_search_posts WHERE username = %s GROUP BY post_id ORDER BY MAX(taken_at) DESC LIMIT 10 OFFSET %s",
        (username.lower(), offset), fetch="all"
    )
    
    if not parent_rows:
        if client_type != "pyrogram":
            bot_instance.send_message(chat_id, f"ℹ️ <b>No further posts found</b> for @{username}.", parse_mode="HTML")
        return
        
    post_ids = [r[0] for r in parent_rows]
    
    # 2. Query the total count of distinct parent posts
    count_row = db_client.execute_query(
        "SELECT COUNT(DISTINCT post_id) FROM instagram_search_posts WHERE username = %s",
        (username.lower(),), fetch="one"
    )
    total_cached = count_row[0] if count_row else 0
    
    page_num = (offset // 10) + 1
    max_pages = (total_cached + 9) // 10
    
    # 3. Query all media rows matching these 10 parent post IDs
    placeholders = ",".join(["%s"] * len(post_ids))
    # Note: replace placeholder placeholders %s with ? for SQLite when running query
    query = f"SELECT post_id, media_url, media_type, caption, likes_count, comments_count, taken_at FROM instagram_search_posts WHERE username = %s AND post_id IN ({placeholders}) ORDER BY taken_at DESC"
    params = [username.lower()] + post_ids
    
    media_rows = db_client.execute_query(query, tuple(params), fetch="all")
    
    # Group media items by post_id
    from collections import defaultdict
    media_by_post = defaultdict(list)
    post_metadata = {}
    
    for row in media_rows:
        pid, m_url, m_type, cap, likes, comments, taken = row
        if (m_url, m_type) not in media_by_post[pid]:
            media_by_post[pid].append((m_url, m_type))
        if pid not in post_metadata:
            post_metadata[pid] = (cap, likes, comments, taken)
            
    # Process and deliver
    # Preserve chronological parent order when iterating
    for pid in post_ids:
        media_list = media_by_post[pid][:10]
        if not media_list:
            continue
            
        cap, likes, comments, taken = post_metadata[pid]
        
        # Build caption with formatting
        taken_str = datetime.fromtimestamp(taken).strftime('%I:%M %p | %b %d, %Y')
        header = f"📸 <b>Post from @{username.lower()}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        footer = f"\n\n❤️ {likes} likes | 💬 {comments} comments\n🕒 Uploaded: <i>{taken_str}</i>\n🔗 <a href='https://instagram.com/p/{pid}'>View on Instagram</a>"
        
        caption_full = f"{header}{cap if cap else ''}{footer}"
        if len(caption_full) > 1024:
            max_cap = 1024 - len(header) - len(footer) - 10
            caption_full = f"{header}{cap[:max_cap]}...{footer}"
            
        # RULE 1: Carousel / Sidecar (more than 1 media) -> Send as separate dedicated album with caption
        if len(media_list) > 1:
            album = []
            opened_files = []
            for idx, (m_url, m_type) in enumerate(media_list):
                item_cap = caption_full if idx == 0 else ""
                if client_type != "pyrogram":
                    lf = download_media_temp(m_url)
                    if lf:
                        try:
                            f_obj = open(lf, 'rb')
                            opened_files.append((f_obj, lf))
                            media_val = f_obj
                        except:
                            media_val = m_url
                    else:
                        media_val = m_url
                else:
                    media_val = m_url

                if client_type == "pyrogram":
                    from pyrogram.types import InputMediaPhoto as PyPhoto, InputMediaVideo as PyVideo
                    if m_type == "video":
                        album.append(PyVideo(media_val, caption=item_cap, parse_mode="HTML"))
                    else:
                        album.append(PyPhoto(media_val, caption=item_cap, parse_mode="HTML"))
                else:
                    from telebot.types import InputMediaPhoto, InputMediaVideo
                    if m_type == "video":
                        album.append(InputMediaVideo(media_val, caption=item_cap, parse_mode="HTML"))
                    else:
                        album.append(InputMediaPhoto(media_val, caption=item_cap, parse_mode="HTML"))
            
            # Send separate Carousel album
            try:
                if client_type == "pyrogram":
                    async def send_py_album(grp):
                        await bot_instance.send_media_group(chat_id=chat_id, media=grp)
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.run_coroutine_threadsafe(send_py_album(album), loop).result()
                    else:
                        loop.run_until_complete(send_py_album(album))
                else:
                    bot_instance.send_media_group(chat_id=chat_id, media=album)
            finally:
                for f_obj, lf in opened_files:
                    try:
                        f_obj.close()
                    except:
                        pass
                    try:
                        os.remove(lf)
                    except:
                        pass
            time.sleep(1)
                
        # RULE 2: Reel (exactly 1 video) -> Send separately with caption
        elif len(media_list) == 1 and media_list[0][1] == "video":
            v_url = media_list[0][0]
            if client_type == "pyrogram":
                async def send_py_video():
                    await bot_instance.send_video(chat_id=chat_id, video=v_url, caption=caption_full, parse_mode="HTML")
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.run_coroutine_threadsafe(send_py_video(), loop).result()
                else:
                    loop.run_until_complete(send_py_video())
            else:
                lf = download_media_temp(v_url)
                if lf:
                    try:
                        with open(lf, 'rb') as f:
                            bot_instance.send_video(chat_id=chat_id, video=f, caption=caption_full, parse_mode="HTML")
                    finally:
                        try:
                            os.remove(lf)
                        except:
                            pass
                else:
                    bot_instance.send_video(chat_id=chat_id, video=v_url, caption=caption_full, parse_mode="HTML")
            time.sleep(1)
                
        # RULE 3: Single Photo -> Send separately with caption
        elif len(media_list) == 1 and media_list[0][1] == "image":
            img_url = media_list[0][0]
            if client_type == "pyrogram":
                async def send_py_photo():
                    await bot_instance.send_photo(chat_id=chat_id, photo=img_url, caption=caption_full, parse_mode="HTML")
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.run_coroutine_threadsafe(send_py_photo(), loop).result()
                else:
                    loop.run_until_complete(send_py_photo())
            else:
                lf = download_media_temp(img_url)
                if lf:
                    try:
                        with open(lf, 'rb') as f:
                            bot_instance.send_photo(chat_id=chat_id, photo=f, caption=caption_full, parse_mode="HTML")
                    finally:
                        try:
                            os.remove(lf)
                        except:
                            pass
                else:
                    bot_instance.send_photo(chat_id=chat_id, photo=img_url, caption=caption_full, parse_mode="HTML")
            time.sleep(1)
        
    # 3. Send Companion message with pagination keyboard if there's more
    cursor = get_instagram_setting(f"cursor_{username.lower()}")
    
    bot_user = resolve_bot_username_robust(bot_instance)
        
    has_more_cached = (offset + 10 < total_cached)
    
    # If we are at the end of the cache, but we have a cursor, we can fetch more
    show_fetch_more = (not has_more_cached and cursor is not None)
    
    if has_more_cached or show_fetch_more:
        if client_type == "pyrogram":
            from pyrogram.types import InlineKeyboardMarkup as PyInlineKeyboardMarkup, InlineKeyboardButton as PyInlineKeyboardButton
            markup = None
            if bot_user:
                if has_more_cached:
                    markup = PyInlineKeyboardMarkup([
                        [PyInlineKeyboardButton("Next 10 Posts ➡️", url=f"https://t.me/{bot_user}?start=search_{username}_{offset+10}")]
                    ])
                elif show_fetch_more:
                    markup = PyInlineKeyboardMarkup([
                        [PyInlineKeyboardButton("🔄 Fetch Next 100 Posts", url=f"https://t.me/{bot_user}?start=fetch_more_{username}")]
                    ])
                
            async def send_py_nav():
                if show_fetch_more:
                    text_msg = f"🔄 Reach end of cached posts. Tap below to fetch the next batch from Instagram:\n\n👉 <b>Fetch More:</b> <code>insta {username} {offset+10}</code>"
                else:
                    text_msg = f"👇 Tap below or send <code>insta {username} {offset+10}</code> to get the next page (Page {page_num+1}/{max_pages}):"
                    text_msg += f"\n\n👉 <b>Next Page:</b> <code>insta {username} {offset+10}</code>"
                await bot_instance.send_message(
                    chat_id=chat_id,
                    text=text_msg,
                    reply_markup=markup,
                    parse_mode="HTML"
                )
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(send_py_nav(), loop).result()
            else:
                loop.run_until_complete(send_py_nav())
        else:
            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
            markup = InlineKeyboardMarkup()
            if has_more_cached:
                markup.add(InlineKeyboardButton("Next 10 Posts ➡️", callback_data=f"instagram_search_page:{username}:{offset+10}"))
                text_msg = f"👇 Click below to load the next 10 posts for @{username} (Page {page_num}/{max_pages}):"
            else:
                markup.add(InlineKeyboardButton("🔄 Fetch Next 100 Posts", callback_data=f"instagram_fetch_next_batch:{username}"))
                text_msg = f"🔄 Reached end of cached posts. Click below to fetch the next batch from Instagram for @{username}:"
                
            markup.row(InlineKeyboardButton("⬅️ Back to History Details", callback_data=f"instagram_history_view:{username}"))
            bot_instance.send_message(
                chat_id,
                text_msg,
                reply_markup=markup,
                parse_mode="HTML"
            )


def handle_instagram_inputs(bot_instance, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip() if message.text else ""
    
    state = user_states.get(user_id)
    if not state or not str(state).startswith("WAITING_FOR_INSTAGRAM_"):
        return False
        
    if text.lower() == "/cancel":
        user_states[user_id] = None
        bot_instance.reply_to(message, "🚫 Action cancelled.", parse_mode="HTML")
        
        if ":" in str(state):
            try:
                target_id = int(str(state).split(":")[-1])
                class CallMock:
                    def __init__(self, from_user, message, cid):
                        self.from_user = from_user
                        self.message = message
                        self.data = cid
                        self.id = "0"
                handle_instagram_callbacks(bot_instance, CallMock(message.from_user, message, f"instagram_view_target:{target_id}"))
                return True
            except:
                pass
                
        bot_instance.send_message(
            chat_id,
            "📸 <b>INSTAGRAM TRACKING PANEL</b>\n━━━━━━━━━━━━━━━━━━━━\nSelect an Instagram ID to track:",
            reply_markup=get_instagram_menu_markup(),
            parse_mode="HTML"
        )
        return True

    # Process Adding Username
    if state == "WAITING_FOR_INSTAGRAM_ID":
        username = text.lstrip("@").strip()
        if not username:
            bot_instance.reply_to(message, "❌ Invalid input. Please send a valid username or type `/cancel`.")
            return True
            
        user_states[user_id] = None
        
        row = db_client.execute_query("SELECT id FROM instagram_targets WHERE username = %s", (username.lower(),), fetch="one")
        if row:
            bot_instance.reply_to(message, f"⚠️ <b>@{username} is already tracked!</b>", parse_mode="HTML")
            bot.send_message(
                chat_id,
                "📸 <b>INSTAGRAM TRACKING PANEL</b>\n━━━━━━━━━━━━━━━━━━━━\nSelect an Instagram ID to track:",
                reply_markup=get_instagram_menu_markup(),
                parse_mode="HTML"
            )
            return True
            
        status_msg = bot_instance.reply_to(message, f"⏳ <b>Fetching profile for @{username} from Instagram...</b>", parse_mode="HTML")
        
        try:
            profile = execute_with_key_rotation("get_user_info", username)
            
            db_client.execute_query(
                "INSERT INTO instagram_targets (username, full_name, biography, profile_pic_url, followers_count, following_count, posts_count) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    profile.get("username", username.lower()).lower(),
                    profile.get("full_name", ""),
                    profile.get("biography", ""),
                    profile.get("profile_pic_url", ""),
                    profile.get("followers_count", 0),
                    profile.get("following_count", 0),
                    profile.get("posts_count", 0)
                ),
                commit=True
            )
            
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
            bot_instance.send_message(chat_id, f"✅ <b>Successfully added @{username}!</b> Details cached in database.", parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Failed to fetch profile: {e}")
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
            bot_instance.send_message(
                chat_id, 
                f"❌ <b>Failed to add target:</b> {e}\n\n"
                f"<i>Note: If you have no keys registered, we fell back to Mock, which should succeed. Verify your keys list in 🔑 Insta API.</i>", 
                parse_mode="HTML"
            )
            
        bot_instance.send_message(
            chat_id,
            "📸 <b>INSTAGRAM TRACKING PANEL</b>\n━━━━━━━━━━━━━━━━━━━━\nSelect an Instagram ID to track:",
            reply_markup=get_instagram_menu_markup(),
            parse_mode="HTML"
        )
        return True

    # Process Setting Chat ID
    elif str(state).startswith("WAITING_FOR_INSTAGRAM_CHAT_ID"):
        user_states[user_id] = None
        target_id = None
        if ":" in str(state):
            try:
                target_id = int(str(state).split(":")[-1])
            except:
                pass
                
        target_chat = text
        if text.lower() == "/current":
            target_chat = str(chat_id)
            
        if not target_chat:
            bot_instance.reply_to(message, "❌ Invalid input. Setting notification chat aborted.")
            return True
            
        set_instagram_setting("notification_chat_id", target_chat)
        bot_instance.reply_to(message, f"✅ <b>Target Chat ID configured to:</b> <code>{target_chat}</code>", parse_mode="HTML")
        
        if target_id:
            class CallMock:
                def __init__(self, from_user, message, cid):
                    self.from_user = from_user
                    self.message = message
                    self.data = cid
                    self.id = "0"
            handle_instagram_callbacks(bot_instance, CallMock(message.from_user, message, f"instagram_ia_config_menu:{target_id}"))
        else:
            bot_instance.send_message(
                chat_id,
                "📸 <b>INSTAGRAM TRACKING PANEL</b>\n━━━━━━━━━━━━━━━━━━━━\nSelect an Instagram ID to track:",
                reply_markup=get_instagram_menu_markup(),
                parse_mode="HTML"
            )
        return True

    # Process Search Query
    elif state == "WAITING_FOR_INSTAGRAM_SEARCH":
        username = text.lstrip("@").strip()
        if not username:
            bot_instance.reply_to(message, "❌ Invalid input. Please send a valid username or type `/cancel`.")
            return True
            
        user_states[user_id] = None
        status_msg = bot_instance.reply_to(message, f"⏳ <b>Scraping @{username} profile and latest posts...</b>", parse_mode="HTML")
        
        try:
            profile = execute_with_key_rotation("get_user_info", username)
            posts, next_c = execute_with_key_rotation("get_latest_posts", username)
            
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
                
            caption = (
                f"🔍 <b>Searched Profile: @{profile.get('username', username)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📛 <b>Name:</b> {profile.get('full_name', 'None')}\n"
                f"📝 <b>Bio:</b> <i>{profile.get('biography', 'None')}</i>\n\n"
                f"👥 <b>Followers:</b> <code>{profile.get('followers_count', 0):,}</code>\n"
                f"🔄 <b>Following:</b> <code>{profile.get('following_count', 0):,}</code>\n"
                f"📸 <b>Posts:</b> <code>{profile.get('posts_count', 0):,}</code>"
            )
            
            pic_url = profile.get("profile_pic_url")
            if pic_url:
                lf = download_media_temp(pic_url)
                if lf:
                    try:
                        with open(lf, 'rb') as f:
                            bot_instance.send_photo(chat_id, f, caption=caption, parse_mode="HTML")
                        lf = None # avoid duplicate attempts
                    except Exception:
                        pass
                    finally:
                        if lf:
                            try:
                                os.remove(lf)
                            except:
                                pass
                if not lf:
                    pass
                else:
                    try:
                        bot_instance.send_photo(chat_id, pic_url, caption=caption, parse_mode="HTML")
                    except Exception:
                        bot_instance.send_message(chat_id, f"🖼️ <a href='{pic_url}'>Profile Photo</a>\n\n{caption}", parse_mode="HTML", disable_web_page_preview=True)
            else:
                bot_instance.send_message(chat_id, caption, parse_mode="HTML")
                
            # Upsert into search profile history
            db_client.execute_query("DELETE FROM instagram_search_profiles WHERE username = %s", (username.lower(),), commit=True)
            db_client.execute_query(
                "INSERT INTO instagram_search_profiles (username, full_name, biography, profile_pic_url, followers_count, following_count, posts_count) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    username.lower(),
                    profile.get("full_name", ""),
                    profile.get("biography", ""),
                    profile.get("profile_pic_url", ""),
                    profile.get("followers_count", 0),
                    profile.get("following_count", 0),
                    profile.get("posts_count", 0)
                ),
                commit=True
            )
            
            # Store pagination cursor
            if next_c:
                set_instagram_setting(f"cursor_{username.lower()}", next_c)
            else:
                db_client.execute_query("DELETE FROM instagram_settings WHERE key = %s", (f"cursor_{username.lower()}",), commit=True)
                
            # Keep history: only delete this specific user's posts to refresh
            db_client.execute_query("DELETE FROM instagram_search_posts WHERE username = %s", (username.lower(),), commit=True)
            
            for post in posts:
                db_client.execute_query(
                    "INSERT INTO instagram_search_posts (username, post_id, media_url, media_type, caption, likes_count, comments_count, taken_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        username.lower(),
                        post.get("id", ""),
                        post.get("media_url", ""),
                        post.get("media_type", "image"),
                        post.get("caption", ""),
                        post.get("likes_count", 0),
                        post.get("comments_count", 0),
                        post.get("taken_at", int(time.time()))
                    ),
                    commit=True
                )
                
            send_search_posts_page(bot_instance, chat_id, username, offset=0)
            
        except Exception as e:
            logger.error(f"Error executing search for @{username}: {e}")
            try:
                bot_instance.delete_message(chat_id, status_msg.message_id)
            except:
                pass
            bot_instance.send_message(
                chat_id, 
                f"❌ <b>Search failed:</b> {e}\n\n"
                f"<i>Verify the username exists and check your registered API keys list.</i>", 
                parse_mode="HTML"
            )
            
            bot_instance.send_message(
                chat_id,
                "📸 <b>INSTAGRAM TRACKING PANEL</b>\n━━━━━━━━━━━━━━━━━━━━\nSelect an Instagram ID to track:",
                reply_markup=get_instagram_menu_markup(),
                parse_mode="HTML"
            )
        return True

    # Process Setting API Key
    elif state == "WAITING_FOR_INSTAGRAM_API_KEY":
        user_states[user_id] = None
        
        parts = text.split(":")
        if len(parts) >= 3:
            api_key = parts[0].strip()
            provider = parts[1].strip().lower()
            host = parts[2].strip().lower()
        else:
            api_key = text.strip()
            provider = "instagram-best-experience"
            host = "instagram-best-experience.p.rapidapi.com"
            
        if api_key:
            # Check if user sent a URL containing a token
            if "token=" in api_key:
                try:
                    from urllib.parse import urlparse, parse_qs
                    parsed = urlparse(api_key)
                    queries = parse_qs(parsed.query)
                    if "token" in queries:
                        api_key = queries["token"][0]
                except:
                    parts_url = api_key.split("token=")
                    if len(parts_url) > 1:
                        api_key = parts_url[1].split("&")[0]
            api_key = api_key.strip("'\" \t\r\n")
            
        if not api_key:
            bot_instance.reply_to(message, "❌ Invalid input. Setting API Key aborted.")
            return True
            
        try:
            db_client.execute_query(
                "INSERT INTO instagram_api_keys (api_key, provider, host) VALUES (%s, %s, %s)",
                (api_key, provider, host),
                commit=True
            )
            bot_instance.reply_to(message, "✅ <b>New API Key added successfully!</b>", parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Error adding duplicate key: {e}")
            bot_instance.reply_to(message, "⚠️ This API Key is already registered or invalid.", parse_mode="HTML")
            
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(message.from_user, message, "instagram_api_menu"))
        return True

    elif state == "WAITING_FOR_INSTAGRAM_APIFY_KEY":
        user_states[user_id] = None
        raw_key = text.strip()
        api_key = raw_key
        
        # Check if user sent a URL containing a token
        if "token=" in raw_key:
            try:
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(raw_key)
                queries = parse_qs(parsed.query)
                if "token" in queries:
                    api_key = queries["token"][0]
            except:
                parts_url = raw_key.split("token=")
                if len(parts_url) > 1:
                    api_key = parts_url[1].split("&")[0]
        api_key = api_key.strip("'\" \t\r\n")
        
        provider = "apify"
        host = "apify.com"
        
        if not api_key:
            bot_instance.reply_to(message, "❌ Invalid input. Setting Apify API Key aborted.")
            return True
            
        try:
            db_client.execute_query(
                "INSERT INTO instagram_api_keys (api_key, provider, host) VALUES (%s, %s, %s)",
                (api_key, provider, host),
                commit=True
            )
            bot_instance.reply_to(message, "✅ <b>New Apify API Key added successfully!</b>", parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Error adding duplicate key: {e}")
            bot_instance.reply_to(message, "⚠️ This API Key is already registered or invalid.", parse_mode="HTML")
            
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(message.from_user, message, "instagram_api_menu"))
        return True

    elif state == "WAITING_FOR_INSTAGRAM_HIKERAPI_KEY":
        user_states[user_id] = None
        api_key = text.strip().strip("'\" \t\r\n")
        provider = "hikerapi"
        host = "hikerapi.com"
        
        if not api_key:
            bot_instance.reply_to(message, "❌ Invalid input. Setting HikerAPI Key aborted.")
            return True
            
        try:
            db_client.execute_query(
                "INSERT INTO instagram_api_keys (api_key, provider, host) VALUES (%s, %s, %s)",
                (api_key, provider, host),
                commit=True
            )
            bot_instance.reply_to(message, "✅ <b>New HikerAPI Key added successfully!</b>", parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Error adding duplicate key: {e}")
            bot_instance.reply_to(message, "⚠️ This API Key is already registered or invalid.", parse_mode="HTML")
            
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(message.from_user, message, "instagram_api_menu"))
        return True

    elif state == "WAITING_FOR_INSTAGRAM_APIFY_COOKIE":
        user_states[user_id] = None
        raw_cookie = text.strip()
        clean_cookie = raw_cookie.strip("'\" \t\r\n")
        
        if not clean_cookie:
            bot_instance.reply_to(message, "❌ Invalid input. Setting Apify Session Cookie aborted.")
            return True
            
        set_instagram_setting("apify_session_cookie", clean_cookie)
        bot_instance.reply_to(message, "✅ <b>Apify Session Cookie configured successfully!</b>", parse_mode="HTML")
        
        class CallMock:
            def __init__(self, from_user, message, cid):
                self.from_user = from_user
                self.message = message
                self.data = cid
                self.id = "0"
        handle_instagram_callbacks(bot_instance, CallMock(message.from_user, message, "instagram_api_menu"))
        return True

    return False


# --- DEEP LINK INTERCEPTOR ON MAIN BOT ---
def deep_link_handler(message):
    text = message.text or ""
    parts = text.split()
    if len(parts) > 1:
        payload = parts[1]
        if payload.startswith("search_"):
            subparts = payload.split("_")
            username = subparts[1]
            offset = int(subparts[2])
            if bot is not None:
                status = bot.send_message(message.chat.id, "⏳ <b>Loading next posts...</b>", parse_mode="HTML")
                try:
                    bot.delete_message(message.chat.id, status.message_id)
                except:
                    pass
                send_search_posts_page(bot, message.chat.id, username, offset)
            return True
        elif payload.startswith("fetch_more_"):
            username = payload.split("_")[2]
            class CallMock:
                def __init__(self, from_user, message, cid):
                    self.from_user = from_user
                    self.message = message
                    self.data = cid
                    self.id = "0"
            if bot is not None:
                handle_instagram_callbacks(bot, CallMock(message.from_user, message, f"instagram_fetch_next_batch:{username}"))
            return True
    return False


# --- DIRECT TELEGRAM COMMAND HANDLER FOR ADMINS / MANAGERS ---
def handle_direct_insta_command(tg_bot, msg):
    parts = msg.text.strip().split()
    if len(parts) < 2:
        tg_bot.reply_to(msg, "❌ Please specify a username. Usage: <code>insta username</code> or <code>/insta username</code>", parse_mode="HTML")
        return
        
    username = parts[1].lstrip("@").strip()
    status_msg = tg_bot.reply_to(msg, f"⏳ <b>Scraping @{username} profile and latest posts...</b>", parse_mode="HTML")
    
    try:
        profile = execute_with_key_rotation("get_user_info", username)
        posts, next_c = execute_with_key_rotation("get_latest_posts", username)
        
        try:
            tg_bot.delete_message(msg.chat.id, status_msg.message_id)
        except:
            pass
            
        caption = (
            f"🔍 <b>Searched Profile: @{profile.get('username', username)}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>Name:</b> {profile.get('full_name', 'None')}\n"
            f"📝 <b>Bio:</b> <i>{profile.get('biography', 'None')}</i>\n\n"
            f"👥 <b>Followers:</b> <code>{profile.get('followers_count', 0):,}</code>\n"
            f"🔄 <b>Following:</b> <code>{profile.get('following_count', 0):,}</code>\n"
            f"📸 <b>Posts:</b> <code>{profile.get('posts_count', 0):,}</code>"
        )
        
        pic_url = profile.get("profile_pic_url")
        if pic_url:
            lf = download_media_temp(pic_url)
            if lf:
                try:
                    with open(lf, 'rb') as f:
                        tg_bot.send_photo(msg.chat.id, f, caption=caption, parse_mode="HTML")
                    lf = None
                except Exception:
                    pass
                finally:
                    if lf:
                        try:
                            os.remove(lf)
                        except:
                            pass
            if not lf:
                pass
            else:
                try:
                    tg_bot.send_photo(msg.chat.id, pic_url, caption=caption, parse_mode="HTML")
                except Exception:
                    tg_bot.send_message(msg.chat.id, f"🖼️ <a href='{pic_url}'>Profile Photo</a>\n\n{caption}", parse_mode="HTML", disable_web_page_preview=True)
        else:
            tg_bot.send_message(msg.chat.id, caption, parse_mode="HTML")
            
        # Upsert history
        db_client.execute_query("DELETE FROM instagram_search_profiles WHERE username = %s", (username.lower(),), commit=True)
        db_client.execute_query(
            "INSERT INTO instagram_search_profiles (username, full_name, biography, profile_pic_url, followers_count, following_count, posts_count) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                username.lower(),
                profile.get("full_name", ""),
                profile.get("biography", ""),
                profile.get("profile_pic_url", ""),
                profile.get("followers_count", 0),
                profile.get("following_count", 0),
                profile.get("posts_count", 0)
            ),
            commit=True
        )
        
        if next_c:
            set_instagram_setting(f"cursor_{username.lower()}", next_c)
        else:
            db_client.execute_query("DELETE FROM instagram_settings WHERE key = %s", (f"cursor_{username.lower()}",), commit=True)
            
        # Refresh posts cache
        db_client.execute_query("DELETE FROM instagram_search_posts WHERE username = %s", (username.lower(),), commit=True)
        
        for post in posts:
            db_client.execute_query(
                "INSERT INTO instagram_search_posts (username, post_id, media_url, media_type, caption, likes_count, comments_count, taken_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    username.lower(),
                    post.get("id", ""),
                    post.get("media_url", ""),
                    post.get("media_type", "image"),
                    post.get("caption", ""),
                    post.get("likes_count", 0),
                    post.get("comments_count", 0),
                    post.get("taken_at", int(time.time()))
                ),
                commit=True
            )
            
        send_search_posts_page(tg_bot, msg.chat.id, username, offset=0)
        
    except Exception as e:
        logger.error(f"Error executing direct command search for @{username}: {e}")
        try:
            tg_bot.delete_message(msg.chat.id, status_msg.message_id)
        except:
            pass
        tg_bot.send_message(
            msg.chat.id, 
            f"❌ <b>Search failed:</b> {e}\n\n"
            f"<i>Verify the username exists and check your registered API keys.</i>", 
            parse_mode="HTML"
        )


# --- DYNAMIC REGISTER FUNCTION ---
def register_handlers(tg_bot):
    tg_bot.register_callback_query_handler(
        lambda call: handle_instagram_callbacks(tg_bot, call),
        func=lambda call: call.data.startswith("instagram_")
    )
    if hasattr(tg_bot, "callback_query_handlers") and len(tg_bot.callback_query_handlers) > 0:
        tg_bot.callback_query_handlers.insert(0, tg_bot.callback_query_handlers.pop())

    tg_bot.register_message_handler(
        lambda msg: handle_instagram_inputs(tg_bot, msg),
        content_types=['text', 'photo', 'audio', 'document', 'video', 'video_note', 'voice', 'location', 'contact', 'sticker'],
        func=lambda msg: msg.from_user.id in user_states and str(user_states.get(msg.from_user.id)).startswith("WAITING_FOR_INSTAGRAM_")
    )
    if hasattr(tg_bot, "message_handlers") and len(tg_bot.message_handlers) > 0:
        tg_bot.message_handlers.insert(0, tg_bot.message_handlers.pop())

    # Direct insta/inta command registration for admins/managers
    tg_bot.register_message_handler(
        lambda msg: handle_direct_insta_command(tg_bot, msg),
        content_types=['text'],
        func=lambda msg: msg.text and (
            any(msg.text.strip().lower().startswith(p) for p in [
                "insta ", "/insta ", ".insta ",
                "inta ", "/inta ", ".inta "
            ]) or 
            msg.text.strip().lower() in [
                "insta", "/insta", ".insta",
                "inta", "/inta", ".inta"
            ]
        )
    )
    if hasattr(tg_bot, "message_handlers") and len(tg_bot.message_handlers) > 0:
        tg_bot.message_handlers.insert(0, tg_bot.message_handlers.pop())


# --- DELAYED ASYNCHRONOUS HOOK INTEGRATION FOR MAIN USERBOT ---
def delayed_integration_worker():
    logger.info("Asynchronous Integration Worker thread started. Waiting for main bot client initialization...")
    # Attempt to locate initialized module and bot client up to 30 times (30 seconds)
    for attempt in range(30):
        time.sleep(1)
        for module_name in ["__main__", "userbot", "bot", "userbot_v2", "userbot_v3", "main"]:
            try:
                # First, check if module has been imported and is in sys.modules
                mod = sys.modules.get(module_name)
                
                # If not in sys.modules yet, try loading it dynamically
                if mod is None and module_name != "__main__":
                    try:
                        mod = __import__(module_name)
                    except Exception:
                        pass
                        
                if mod is not None:
                    client = getattr(mod, "bot", None) or getattr(mod, "app", None) or getattr(mod, "client", None)
                    if client is not None:
                        global bot, user_states, is_standalone, main_module
                        bot = client
                        user_states = getattr(mod, "user_states", user_states)
                        is_standalone = False
                        main_module = mod
                        
                        try:
                            bot_me = bot.get_me()
                            if bot_me and bot_me.username:
                                set_instagram_setting("bot_username", bot_me.username)
                                logger.info(f"Auto-saved main bot username: @{bot_me.username}")
                        except Exception as e:
                            logger.error(f"Failed to fetch bot username in integration worker: {e}")
                        
                        # Monkey-patch keyboard admin panel / dashboard markup builders
                        patched = False
                        for fn_name in ["get_dashboard_markup", "get_admin_panel_markup", "_panel_main_markup"]:
                            if hasattr(mod, fn_name):
                                original_markup_fn = getattr(mod, fn_name)
                                
                                def make_patched_fn(orig_fn):
                                    def patched_markup_fn(*args, **kwargs):
                                        markup = orig_fn(*args, **kwargs)
                                        from telebot.types import InlineKeyboardButton
                                        markup.row(InlineKeyboardButton("📱 Instagram Scraper", callback_data="instagram_main_menu"))
                                        return markup
                                    return patched_markup_fn
                                    
                                setattr(mod, fn_name, make_patched_fn(original_markup_fn))
                                logger.info(f"Successfully monkey-patched dashboard markup builder: {fn_name}")
                                patched = True
                                
                        if not patched:
                            logger.warning("No dashboard or admin panel markup function found to monkey-patch.")
                        
                        # Register handlers
                        register_handlers(bot)
                        
                        # Prepend deep link command handler to index 0 of message_handlers
                        if hasattr(bot, "register_message_handler") and hasattr(bot, "message_handlers"):
                            bot.register_message_handler(
                                deep_link_handler,
                                commands=["start"],
                                func=lambda msg: bool(msg.text and len(msg.text.split()) > 1 and msg.text.split()[1].startswith("search_"))
                            )
                            bot.message_handlers.insert(0, bot.message_handlers.pop())
                            
                        logger.info(f"✅ Instagram Scraper plugin successfully injected and registered with '{module_name}' bot.")
                        return
            except Exception as e:
                logger.error(f"Error in delayed integration worker attempt {attempt}: {e}")
                
    logger.info("Delayed integration worker scan completed. Main bot not found or not initialized in time. Operating in standalone mode.")

threading.Thread(target=delayed_integration_worker, daemon=True).start()


# --- AUTOMATED MEDIA ALERT ROUTER (USERBOT PREFERRED + BOT FALLBACK) ---
def send_auto_media_to_destination(destination_id, media_url, media_type, caption):
    sent_via_userbot = False
    dest_id_str = str(destination_id).strip()
    
    # Try sending via any active userbot Client
    for uid, client in list(running_userbots.items()):
        try:
            dest = int(dest_id_str) if dest_id_str.replace("-", "").isdigit() else dest_id_str
            
            async def upload_task():
                if media_type == "video":
                    await client.send_video(chat_id=dest, video=media_url, caption=caption)
                else:
                    await client.send_photo(chat_id=dest, photo=media_url, caption=caption)
                    
            loop = client.loop
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(upload_task(), loop).result()
            else:
                loop.run_until_complete(upload_task())
                
            logger.info(f"Sent automated {media_type} alert to {destination_id} via Userbot client {uid}.")
            sent_via_userbot = True
            break
        except Exception as err:
            logger.warning(f"Failed to send alert to {destination_id} via Userbot client {uid}: {err}. Trying next...")
            
    if not sent_via_userbot and bot is not None:
        # Fallback to main bot token client
        try:
            dest = int(dest_id_str) if dest_id_str.replace("-", "").isdigit() else dest_id_str
            if media_type == "video":
                bot.send_video(chat_id=dest, video=media_url, caption=caption, parse_mode="HTML")
            else:
                bot.send_photo(chat_id=dest, photo=media_url, caption=caption, parse_mode="HTML")
            logger.info(f"Sent automated {media_type} alert to {destination_id} via fallback main bot.")
        except Exception as e:
            logger.error(f"Failed to deliver automated alert to {destination_id} via fallback main bot: {e}")


# --- BACKGROUND STORY POLLING WORKER LOOP ---
def auto_story_fetcher_worker():
    time.sleep(12)
    logger.info("Background Auto Story Fetcher worker thread initialized.")
    
    while True:
        try:
            if bot is None:
                time.sleep(10)
                continue
                
            now = time.time()
            
            # --- 1. SCAN IA STORIES POLLING CYCLE ---
            targets_stories = db_client.execute_query(
                "SELECT id, username, ia_interval, last_auto_poll_time FROM instagram_targets WHERE ia_stories = 1",
                fetch="all"
            )
            if targets_stories:
                for target_row in targets_stories:
                    tid, username, ia_interval, last_poll_val = target_row
                    try:
                        last_poll_ts = float(last_poll_val or 0)
                    except:
                        last_poll_ts = 0.0
                        
                    if now - last_poll_ts >= (ia_interval * 60):
                        logger.info(f"Running scheduled IA Stories fetch for @{username} (Interval: {ia_interval}m)...")
                        
                        db_client.execute_query(
                            "UPDATE instagram_targets SET last_auto_poll_time = %s WHERE id = %s",
                            (str(now), tid), commit=True
                        )
                        
                        try:
                            stories = execute_with_key_rotation("get_latest_stories", username)
                            if not stories:
                                continue
                                
                            # Fetch active receivers list
                            receivers = db_client.execute_query(
                                "SELECT telegram_id FROM instagram_target_receivers WHERE target_id = %s AND stories_enabled = 1",
                                (tid,), fetch="all"
                            )
                            dest_list = []
                            if receivers:
                                dest_list = [r[0] for r in receivers]
                            else:
                                chat_id = get_instagram_setting("notification_chat_id", "").strip()
                                if chat_id:
                                    dest_list = [chat_id]
                                    
                            if not dest_list:
                                continue
                                
                            for story in stories:
                                story_id = story["id"]
                                
                                seen = db_client.execute_query("SELECT 1 FROM instagram_seen_stories WHERE story_id = %s", (story_id,), fetch="one")
                                if not seen:
                                    db_client.execute_query("INSERT INTO instagram_seen_stories (story_id, username) VALUES (%s, %s)", (story_id, username), commit=True)
                                    
                                    s_url = story["media_url"]
                                    s_type = story["media_type"]
                                    taken_ts = story["taken_at"]
                                    taken_str = datetime.fromtimestamp(taken_ts).strftime('%I:%M %p | %b %d, %Y')
                                    
                                    caption = (
                                        f"⚡ <b>New Story from @{username}!</b>\n"
                                        f"━━━━━━━━━━━━━━━━━━━━\n"
                                        f"🕒 Uploaded: <i>{taken_str}</i>"
                                    )
                                    
                                    for dest in dest_list:
                                        send_auto_media_to_destination(dest, s_url, s_type, caption)
                                        time.sleep(1)
                                    time.sleep(2)
                                    
                        except Exception as scrape_err:
                            logger.error(f"Auto-fetcher stories error scanning @{username}: {scrape_err}")
            
            # --- 2. SCAN IA POSTS POLLING CYCLE ---
            targets_posts = db_client.execute_query(
                "SELECT id, username, ia_post_interval, last_auto_post_poll_time FROM instagram_targets WHERE ia_posts = 1",
                fetch="all"
            )
            if targets_posts:
                for target_row in targets_posts:
                    tid, username, ia_post_interval, last_post_poll_val = target_row
                    try:
                        last_post_poll_ts = float(last_post_poll_val or 0)
                    except:
                        last_post_poll_ts = 0.0
                        
                    if now - last_post_poll_ts >= (ia_post_interval * 60):
                        logger.info(f"Running scheduled IA Posts fetch for @{username} (Interval: {ia_post_interval}m)...")
                        
                        db_client.execute_query(
                            "UPDATE instagram_targets SET last_auto_post_poll_time = %s WHERE id = %s",
                            (str(now), tid), commit=True
                        )
                        
                        try:
                            posts, next_c = execute_with_key_rotation("get_latest_posts", username)
                            if not posts:
                                continue
                                
                            # Fetch active receivers list
                            receivers = db_client.execute_query(
                                "SELECT telegram_id FROM instagram_target_receivers WHERE target_id = %s AND posts_enabled = 1",
                                (tid,), fetch="all"
                            )
                            dest_list = []
                            if receivers:
                                dest_list = [r[0] for r in receivers]
                            else:
                                chat_id = get_instagram_setting("notification_chat_id", "").strip()
                                if chat_id:
                                    dest_list = [chat_id]
                                    
                            if not dest_list:
                                continue
                                
                            for post in posts:
                                post_id = post["id"]
                                
                                seen = db_client.execute_query("SELECT 1 FROM instagram_seen_posts WHERE post_id = %s", (post_id,), fetch="one")
                                if not seen:
                                    db_client.execute_query("INSERT INTO instagram_seen_posts (post_id, username) VALUES (%s, %s)", (post_id, username), commit=True)

                                    p_url = post["media_url"]
                                    p_type = post["media_type"]
                                    p_caption = post["caption"]
                                    taken_ts = post["taken_at"]
                                    taken_str = datetime.fromtimestamp(taken_ts).strftime('%I:%M %p | %b %d, %Y')
                                    
                                    header = f"📸 <b>New Post from @{username}!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                                    footer = f"\n\n❤️ {post.get('likes_count', 0)} likes | 💬 {post.get('comments_count', 0)} comments\n🕒 Uploaded: <i>{taken_str}</i>\n🔗 <a href='https://instagram.com/p/{post_id}'>View on Instagram</a>"
                                    
                                    max_body = 1024 - len(header) - len(footer) - 10
                                    caption_body = p_caption if p_caption else ""
                                    if len(caption_body) > max_body:
                                        caption_body = caption_body[:max_body] + "..."
                                    caption_full = f"{header}{caption_body}{footer}"
                                    
                                    for dest in dest_list:
                                        send_auto_media_to_destination(dest, p_url, p_type, caption_full)
                                        time.sleep(1)
                                    time.sleep(2)
                                    
                        except Exception as scrape_err:
                            logger.error(f"Auto-fetcher posts error scanning @{username}: {scrape_err}")
                            
        except Exception as e:
            logger.error(f"Auto-fetcher general loop exception: {e}")
            
        time.sleep(15) # Scan intervals database updates check every 15s


# --- BACKGROUND TELETHON LISTENERS LOOP ---
from telethon import events
import tempfile

def download_media_temp(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        res = requests.get(url, headers=headers, timeout=15, stream=True)
        if res.status_code == 200:
            suffix = ".jpg"
            if "video" in res.headers.get("Content-Type", ""):
                suffix = ".mp4"
            elif ".mp4" in url:
                suffix = ".mp4"
                
            fd, path = tempfile.mkstemp(suffix=suffix)
            with os.fdopen(fd, 'wb') as tmp:
                for chunk in res.iter_content(chunk_size=8192):
                    if chunk:
                        tmp.write(chunk)
            return path
    except Exception as e:
        logger.error(f"Failed to download temp media: {e}")
    return None

async def telethon_handle_insta_command(event):
    text = event.message.message or ""
    parts = text.strip().split()
    if not parts:
        return
        
    cmd = parts[0].lower()
    # Handle all prefix combinations (including test command)
    if cmd not in [".insta", ".inta", "/insta", "/inta", "insta", "inta", ".test", "/test", "test"]:
        return
        
    sender = await event.get_sender()
    sender_id = sender.id if sender else event.chat_id
    
    client = event.client
    me = await client.get_me()
    
    # Simple command logger
    try:
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instagram_userbot_commands.log")
        with open(log_path, "a", encoding="utf-8") as log_f:
            log_f.write(f"[{datetime.now()}] User ID: {sender_id} | Command: {text} | Client: {me.first_name} (@{me.username})\n")
    except Exception as log_err:
        logger.error(f"Failed to write to userbot commands log: {log_err}")
    
    # Authorization check (must be client self, admin, manager, or linked userbot)
    is_allowed = False
    if sender_id == me.id:
        is_allowed = True
    else:
        # 1. Check managers table directly
        try:
            row = db_client.execute_query("SELECT 1 FROM managers WHERE user_id = %s", (str(sender_id),), fetch="one")
            if row:
                is_allowed = True
        except Exception:
            pass
            
        # 2. Check userbot_sessions or linked_userbots table directly
        if not is_allowed:
            try:
                table_to_check = "userbot_sessions" if db_client.table_exists("userbot_sessions") else "linked_userbots"
                row = db_client.execute_query(f"SELECT 1 FROM {table_to_check} WHERE user_id = %s", (str(sender_id),), fetch="one")
                if row:
                    is_allowed = True
            except Exception:
                pass
                
        # 3. Dynamic modules check fallback (checking ADMIN_ID, is_admin, or is_authorized_manager)
        if not is_allowed:
            for mod_name in ["bot", "userbot", "userbot_v2", "userbot_v3", "main", "__main__"]:
                try:
                    mod = sys.modules.get(mod_name)
                    if mod:
                        if hasattr(mod, "is_admin") and getattr(mod, "is_admin")(sender_id):
                            is_allowed = True
                            break
                        if hasattr(mod, "is_authorized_manager") and getattr(mod, "is_authorized_manager")(sender_id):
                            is_allowed = True
                            break
                        if hasattr(mod, "ADMIN_ID") and int(getattr(mod, "ADMIN_ID")) == int(sender_id):
                            is_allowed = True
                            break
                except Exception:
                    pass
            
    if not is_allowed:
        return
        
    # Handle .test command reply
    if cmd in [".test", "/test", "test"]:
        await event.reply("This is a test message. Userbot Instagram scraper is active!", parse_mode="HTML")
        try:
            log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instagram_userbot_commands.log")
            with open(log_path, "a", encoding="utf-8") as log_f:
                log_f.write(f"[{datetime.now()}] Executed .test reply successfully for User ID {sender_id}\n")
        except:
            pass
        return

    if len(parts) < 2:
        await event.reply(
            "❌ Please specify a username.\nUsage: <code>.insta username</code> or <code>/insta username</code>",
            parse_mode="HTML"
        )
        return
        
    username = parts[1].lstrip("@").strip()
    offset = 0
    if len(parts) >= 3 and parts[2].isdigit():
        offset = int(parts[2])
        
    status = await event.reply(f"⏳ <b>Scraping @{username} profile and posts (Offset: {offset})...</b>", parse_mode="HTML")
    
    try:
        # Run DB query and API call inside thread executor to keep event loop free
        loop = asyncio.get_event_loop()
        
        if offset == 0:
            profile = await loop.run_in_executor(None, lambda: execute_with_key_rotation("get_user_info", username))
            posts, next_c = await loop.run_in_executor(None, lambda: execute_with_key_rotation("get_latest_posts", username))
            
            caption = (
                f"🔍 <b>Searched Profile: @{profile.get('username', username)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📛 <b>Name:</b> {profile.get('full_name', 'None')}\n"
                f"📝 <b>Bio:</b> <i>{profile.get('biography', 'None')}</i>\n\n"
                f"👥 <b>Followers:</b> <code>{profile.get('followers_count', 0):,}</code>\n"
                f"🔄 <b>Following:</b> <code>{profile.get('following_count', 0):,}</code>\n"
                f"📸 <b>Posts:</b> <code>{profile.get('posts_count', 0):,}</code>"
            )
            
            pic_url = profile.get("profile_pic_url")
            if pic_url:
                local_pic = await loop.run_in_executor(None, download_media_temp, pic_url)
                if local_pic:
                    try:
                        await client.send_file(event.chat_id, local_pic, caption=caption, parse_mode="HTML")
                    except Exception as err:
                        logger.error(f"Failed sending local profile pic: {err}")
                        await client.send_message(event.chat_id, f"🖼️ Profile photo link: {pic_url}\n\n{caption}", parse_mode="HTML")
                    finally:
                        try:
                            os.remove(local_pic)
                        except:
                            pass
                else:
                    await client.send_message(event.chat_id, caption, parse_mode="HTML")
            else:
                await client.send_message(event.chat_id, caption, parse_mode="HTML")
                
            db_client.execute_query("DELETE FROM instagram_search_posts WHERE username = %s", (username.lower(),), commit=True)
            
            for post in posts:
                db_client.execute_query(
                    "INSERT INTO instagram_search_posts (username, post_id, media_url, media_type, caption, likes_count, comments_count, taken_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        username.lower(),
                        post.get("id", ""),
                        post.get("media_url", ""),
                        post.get("media_type", "image"),
                        post.get("caption", ""),
                        post.get("likes_count", 0),
                        post.get("comments_count", 0),
                        post.get("taken_at", int(time.time()))
                    ),
                    commit=True
                )
                
            if next_c:
                set_instagram_setting(f"cursor_{username.lower()}", next_c)
            else:
                db_client.execute_query("DELETE FROM instagram_settings WHERE key = %s", (f"cursor_{username.lower()}",), commit=True)
                
        try:
            await status.delete()
        except:
            pass
            
        # Deliver search page album using Telethon
        await async_send_search_posts_page_telethon(client, event.chat_id, username, offset)
        
    except Exception as err:
        logger.error(f"Userbot command execution error: {err}")
        try:
            await status.delete()
        except:
            pass
        await event.reply(f"❌ <b>Search failed:</b> {err}", parse_mode="HTML")


async def async_send_search_posts_page_telethon(client, chat_id, username, offset=0):
    parent_rows = db_client.execute_query(
        "SELECT post_id FROM instagram_search_posts WHERE username = %s GROUP BY post_id ORDER BY MAX(taken_at) DESC LIMIT 10 OFFSET %s",
        (username.lower(), offset), fetch="all"
    )
    
    if not parent_rows:
        await client.send_message(chat_id, f"ℹ️ <b>No further posts found</b> for @{username}.", parse_mode="HTML")
        return
        
    post_ids = [r[0] for r in parent_rows]
    
    count_row = db_client.execute_query(
        "SELECT COUNT(DISTINCT post_id) FROM instagram_search_posts WHERE username = %s",
        (username.lower(),), fetch="one"
    )
    total_cached = count_row[0] if count_row else 0
    
    page_num = (offset // 10) + 1
    max_pages = (total_cached + 9) // 10
    
    placeholders = ",".join(["%s"] * len(post_ids))
    query = f"SELECT post_id, media_url, media_type, caption, likes_count, comments_count, taken_at FROM instagram_search_posts WHERE username = %s AND post_id IN ({placeholders}) ORDER BY taken_at DESC"
    params = [username.lower()] + post_ids
    
    media_rows = db_client.execute_query(query, tuple(params), fetch="all")
    
    from collections import defaultdict
    media_by_post = defaultdict(list)
    post_metadata = {}
    
    for row in media_rows:
        pid, m_url, m_type, cap, likes, comments, taken = row
        if (m_url, m_type) not in media_by_post[pid]:
            media_by_post[pid].append((m_url, m_type))
        if pid not in post_metadata:
            post_metadata[pid] = (cap, likes, comments, taken)
            
    for pid in post_ids:
        media_list = media_by_post[pid][:10]
        if not media_list:
            continue
            
        cap, likes, comments, taken = post_metadata[pid]
        taken_str = datetime.fromtimestamp(taken).strftime('%I:%M %p | %b %d, %Y')
        header = f"📸 <b>Post from @{username.lower()}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        footer = f"\n\n❤️ {likes} likes | 💬 {comments} comments\n🕒 Uploaded: <i>{taken_str}</i>\n🔗 <a href='https://instagram.com/p/{pid}'>View on Instagram</a>"
        
        caption_full = f"{header}{cap if cap else ''}{footer}"
        if len(caption_full) > 1024:
            max_cap = 1024 - len(header) - len(footer) - 10
            caption_full = f"{header}{cap[:max_cap]}...{footer}"
            
        loop = asyncio.get_event_loop()
        if len(media_list) > 1:
            urls = [m[0] for m in media_list]
            local_files = []
            for url in urls:
                lf = await loop.run_in_executor(None, download_media_temp, url)
                if lf:
                    local_files.append(lf)
            if local_files:
                try:
                    await client.send_file(chat_id, local_files, caption=caption_full, parse_mode="HTML")
                finally:
                    for lf in local_files:
                        try:
                            os.remove(lf)
                        except:
                            pass
            await asyncio.sleep(1)
        elif len(media_list) == 1 and media_list[0][1] == "video":
            lf = await loop.run_in_executor(None, download_media_temp, media_list[0][0])
            if lf:
                try:
                    await client.send_file(chat_id, lf, caption=caption_full, parse_mode="HTML")
                finally:
                    try:
                        os.remove(lf)
                    except:
                        pass
            await asyncio.sleep(1)
        elif len(media_list) == 1 and media_list[0][1] == "image":
            lf = await loop.run_in_executor(None, download_media_temp, media_list[0][0])
            if lf:
                try:
                    await client.send_file(chat_id, lf, caption=caption_full, parse_mode="HTML")
                finally:
                    try:
                        os.remove(lf)
                    except:
                        pass
            await asyncio.sleep(1)
            
    cursor = get_instagram_setting(f"cursor_{username.lower()}")
    has_more_cached = (offset + 10 < total_cached)
    show_fetch_more = (not has_more_cached and cursor is not None)
    
    bot_user = resolve_bot_username_robust()
        
    if has_more_cached or show_fetch_more:
        from telethon import Button
        markup = None
        if bot_user:
            if has_more_cached:
                markup = [Button.url("Next 10 Posts ➡️", f"https://t.me/{bot_user}?start=search_{username}_{offset+10}")]
            elif show_fetch_more:
                markup = [Button.url("🔄 Fetch Next 100 Posts", f"https://t.me/{bot_user}?start=fetch_more_{username}")]
                
        if show_fetch_more:
            text_msg = f"🔄 Reached end of cached posts. Tap below to fetch the next batch from Instagram:\n\n👉 <b>Fetch More:</b> <code>.insta {username} {offset+10}</code>"
        else:
            text_msg = f"👇 Tap below to get the next page (Page {page_num+1}/{max_pages}):"
            text_msg += f"\n\n👉 <b>Next Page:</b> <code>.insta {username} {offset+10}</code>"
            
        await client.send_message(chat_id, text_msg, buttons=markup, parse_mode="HTML")


registered_telethon_clients = set()

def userbot_listener_manager_loop():
    logger.info("Background Telethon Userbot Listener Manager loop started.")
    while True:
        try:
            status_info = []
            status_info.append(f"Time: {datetime.now()}")
            status_info.append(f"is_standalone: {is_standalone}")
            status_info.append(f"main_module: {main_module.__name__ if main_module else 'None'}")
            
            fleet_manager = None
            if main_module is not None:
                fleet_manager = getattr(main_module, "userbot_fleet_manager", None)
                status_info.append(f"fleet_manager found in main_module: {fleet_manager is not None}")
            
            # Direct fallback check for sys.modules["__main__"] if main_module resolve failed
            if fleet_manager is None:
                main_mod = sys.modules.get("__main__")
                if main_mod:
                    fleet_manager = getattr(main_mod, "userbot_fleet_manager", None)
                    status_info.append(f"fleet_manager found in sys.modules['__main__'] fallback: {fleet_manager is not None}")
            
            if fleet_manager is not None:
                clients = fleet_manager.get_all_clients()
                status_info.append(f"Number of clients in fleet_manager: {len(clients)}")
                for client in clients:
                    me = getattr(client, "_me", None)
                    me_id = me.id if me else "None"
                    status_info.append(f"  Client ID: {id(client)}, User ID: {me_id}, Connected: {client.is_connected()}")
                    
                    client_id = id(client)
                    if client_id not in registered_telethon_clients:
                        logger.info(f"Registering Instagram command listener on Telethon client {client_id}...")
                        client.add_event_handler(
                            telethon_handle_insta_command,
                            events.NewMessage(incoming=True)
                        )
                        registered_telethon_clients.add(client_id)
            else:
                status_info.append("fleet_manager is None")
                
            status_info.append(f"registered_telethon_clients: {list(registered_telethon_clients)}")
            
            # Write diagnostic file
            try:
                debug_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instagram_debug.txt")
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(status_info))
            except Exception as w_err:
                logger.error(f"Could not write diagnostic file: {w_err}")
                
        except Exception as e:
            logger.error(f"Error in userbot listener manager loop: {e}")
            try:
                debug_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instagram_debug.txt")
                with open(debug_path, "a", encoding="utf-8") as f:
                    f.write(f"\nLoop error: {e}")
            except:
                pass
            
        time.sleep(10)


# Start background loops
threading.Thread(target=auto_story_fetcher_worker, daemon=True).start()
threading.Thread(target=userbot_listener_manager_loop, daemon=True).start()


# --- STANDALONE RUNNER CODE ---
if __name__ == "__main__":
    is_standalone = True
    user_states = {}
    
    token = os.environ.get("BOT_TOKEN")
    if not token:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_json_path = os.path.join(base_dir, "config.json")
        if os.path.exists(config_json_path):
            try:
                with open(config_json_path, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                    token = config_data.get("bot_token")
            except Exception:
                pass

    if not token or "YOUR_BOT_TOKEN" in token:
        logger.critical("Error: Telegram BOT_TOKEN is missing. Please configure it in your environment variables.")
        print("\n=======================================================")
        print("CRITICAL RUNTIME ERROR:")
        print("Please set the 'BOT_TOKEN' environment variable.")
        print("=======================================================\n")
        sys.exit(1)

    bot = TeleBot(token.strip())
    try:
        bot_me = bot.get_me()
        if bot_me and bot_me.username:
            set_instagram_setting("bot_username", bot_me.username)
            logger.info(f"Auto-saved standalone bot username: @{bot_me.username}")
    except Exception as e:
        logger.error(f"Failed to fetch bot username in standalone startup: {e}")
    
    # Standalone Commands
    @bot.message_handler(commands=["start", "admin", "instagram"])
    def cmd_start(message):
        # Check deep link start arguments
        text = message.text or ""
        parts = text.split()
        if len(parts) > 1 and parts[1].startswith("search_"):
            deep_link_handler(message)
            return
            
        menu_text = (
            "📸 <b>INSTAGRAM TRACKING PANEL (Standalone)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Monitor public Instagram accounts and check stories in real-time.\n\n"
            "💡 <i>Tapping a user profile displays details from the database instantly without calling the API.</i>"
        )
        bot.send_message(
            message.chat.id,
            menu_text,
            reply_markup=get_instagram_menu_markup(),
            parse_mode="HTML"
        )

    # Register handlers
    register_handlers(bot)
    
    logger.info("🚀 Starting Standalone Instagram Tracking Bot...")
    try:
        bot.infinity_polling()
    except KeyboardInterrupt:
        logger.info("Standalone bot stopped by user.")
    except Exception as e:
        logger.critical(f"Standalone bot polling failed: {e}")
