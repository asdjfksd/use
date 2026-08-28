import sys
import os
import asyncio
import logging
import re
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon import events, types

logger = logging.getLogger(__name__)

# Find the active running bot module from sys.modules
main_module = None
for name in ['__main__', 'userbot', 'testuserbot_v3']:
    mod = sys.modules.get(name)
    if mod and hasattr(mod, 'get_dashboard_markup'):
        main_module = mod
        break

if main_module is None:
    try:
        import userbot as main_module
    except ImportError:
        import testuserbot_v3 as main_module

bot = main_module.bot
is_user_banned = getattr(main_module, 'is_user_banned', None)
get_dashboard_markup = main_module.get_dashboard_markup
is_authorized_manager = main_module.is_authorized_manager
set_setting = main_module.set_setting
get_setting = main_module.get_setting
admin_states = main_module.admin_states
userbot_fleet_manager = main_module.userbot_fleet_manager
loop = main_module.loop
ADMIN_ID = main_module.ADMIN_ID
get_placeholder = main_module.get_placeholder
db_conn = main_module.db_conn
resolve_target_id = main_module.resolve_target_id
ensure_userbot = main_module.ensure_userbot
get_target_pairs = main_module.get_target_pairs
processed_messages = main_module.processed_messages

def is_postgres():
    return getattr(main_module, 'USING_POSTGRES', False) or bool(getattr(main_module, 'DATABASE_URL', ''))

monitored_keywords = []

# Rate limiting cache to prevent spamming alerts
# Maps (chat_id, keyword) -> last_alert_time
keyword_alert_history = {}

def get_alert_destination():
    dest = get_setting("keyword_check_alert_destination", "")
    if dest and dest.strip():
        try:
            return int(dest.strip())
        except ValueError:
            pass
    return ADMIN_ID

def get_alternate_ids(chat_id):
    ids = {chat_id}
    # For channels/supergroups (e.g. -100123456789)
    if str(chat_id).startswith("-100"):
        try:
            ids.add(int(str(chat_id)[4:]))
        except ValueError: pass
    # For regular group chats (e.g. -123456789)
    elif str(chat_id).startswith("-"):
        try:
            ids.add(int(str(chat_id)[1:]))
        except ValueError: pass
    # Also support converting positive IDs to alternate negative forms
    else:
        try:
            val = int(chat_id)
            ids.add(-val)
            ids.add(int(f"-100{val}"))
        except ValueError: pass
    return ids

# Initialize database schema inside plugin
def init_plugin_db():
    try:
        with db_conn() as conn:
            c = conn.cursor()
            is_pg = is_postgres()
            
            c.execute("""
                CREATE TABLE IF NOT EXISTS keyword_checks (
                    keyword TEXT PRIMARY KEY
                )
            """)
            
            if is_pg:
                c.execute("""
                    CREATE TABLE IF NOT EXISTS keyword_monitored_groups (
                        chat_id BIGINT PRIMARY KEY,
                        title TEXT,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            else:
                c.execute("""
                    CREATE TABLE IF NOT EXISTS keyword_monitored_groups (
                        chat_id BIGINT PRIMARY KEY,
                        title TEXT,
                        added_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to initialize Keyword Monitor database: {e}")

init_plugin_db()

# DB Helpers for keywords
def reload_keywords_cache_local():
    global monitored_keywords
    try:
        monitored_keywords = get_keywords()
    except Exception as e:
        logger.error(f"Failed to reload local keywords cache in plugin: {e}")
        monitored_keywords = []

def reload_keywords_cache():
    reload_keywords_cache_local()
    # Sync with main module
    if hasattr(main_module, 'reload_keywords_cache'):
        try:
            main_module.reload_keywords_cache()
        except Exception as e:
            logger.error(f"Failed to reload main module keywords cache from plugin: {e}")

def add_keyword(word):
    clean_word = word.strip().lower()
    if not clean_word: return
    with db_conn() as conn:
        c = conn.cursor()
        if is_postgres():
            c.execute("INSERT INTO keyword_checks (keyword) VALUES (%s) ON CONFLICT DO NOTHING", (clean_word,))
        else:
            c.execute("INSERT OR IGNORE INTO keyword_checks (keyword) VALUES (?)", (clean_word,))
        conn.commit()
    reload_keywords_cache()

def remove_keyword(word):
    clean_word = word.strip().lower()
    if not clean_word: return
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM keyword_checks WHERE keyword = {p}", (clean_word,))
        conn.commit()
    reload_keywords_cache()

def get_keywords():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT keyword FROM keyword_checks")
        return [row[0] for row in c.fetchall()]

# DB Helpers for groups
def add_keyword_monitored_group(chat_id, title):
    with db_conn() as conn:
        c = conn.cursor()
        if is_postgres():
            c.execute("INSERT INTO keyword_monitored_groups (chat_id, title) VALUES (%s, %s) ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title", (chat_id, title))
        else:
            c.execute("INSERT OR REPLACE INTO keyword_monitored_groups (chat_id, title) VALUES (?, ?)", (chat_id, title))
        conn.commit()

def remove_keyword_monitored_group(chat_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM keyword_monitored_groups WHERE chat_id = {p}", (chat_id,))
        conn.commit()

def get_keyword_monitored_groups():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT chat_id, title FROM keyword_monitored_groups ORDER BY added_at DESC")
        return c.fetchall()

# Reload initial keywords
reload_keywords_cache()

# --- UI Helpers ---
def get_keyword_monitor_text():
    enabled = get_setting("keyword_check_active", "0") == "1"
    status_emoji = "🟢 ENABLED" if enabled else "🔴 DISABLED"
    
    links_enabled = get_setting("keyword_check_links_active", "0") == "1"
    links_emoji = "🟢 ENABLED" if links_enabled else "🔴 DISABLED"
    
    keywords = get_keywords()
    monitored_groups = get_keyword_monitored_groups()
    
    dest_id = get_alert_destination()
    dest_name = "Admin DM"
    if dest_id != ADMIN_ID:
        dest_name = f"Chat ID: {dest_id}"
        for g in monitored_groups:
            if int(g[0]) == dest_id:
                dest_name = f"{g[1]} ({dest_id})"
                break
                
    text = "🔑 *KEYWORD & LINK MONITOR CONSOLE*\n\n"
    text += f"Status: `{status_emoji}`\n"
    text += f"Link Monitor: `{links_emoji}`\n"
    text += f"Alert Destination: `{dest_name}`\n"
    text += f"Configured Keywords: `{len(keywords)}`\n"
    text += f"Monitored Groups: `{len(monitored_groups)}`\n\n"
    text += "This system monitors specified groups/channels for keywords and links. When a match is found, an alert is sent to your configured destination.\n\n"
    text += "*How it decides which groups to monitor:*\n"
    text += "1. Source chats of all active Target Pairs.\n"
    text += "2. Groups explicitly added below for monitoring (no target pair required).\n"
    return text

def get_keyword_monitor_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    enabled = get_setting("keyword_check_active", "0") == "1"
    toggle_label = "🔴 Disable Keyword Check" if enabled else "🟢 Enable Keyword Check"
    
    links_enabled = get_setting("keyword_check_links_active", "0") == "1"
    toggle_links_label = "🔴 Disable Link Monitor" if links_enabled else "🟢 Enable Link Monitor"
    
    markup.add(
        InlineKeyboardButton(toggle_label, callback_data="kw_mon_toggle"),
        InlineKeyboardButton(toggle_links_label, callback_data="kw_mon_toggle_links"),
        InlineKeyboardButton("🎯 Select Alert Destination", callback_data="kw_dest_manage"),
        InlineKeyboardButton("📝 Manage Keywords", callback_data="kw_mon_keywords"),
        InlineKeyboardButton("👥 Manage Monitored Groups", callback_data="kw_mon_groups")
    )
    markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
    return markup

def get_kw_keywords_text():
    keywords = get_keywords()
    text = "📝 *Manage Keywords*\n\n"
    if keywords:
        text += "Here are your currently configured keywords. Messages containing any of these words in monitored groups will trigger an alert.\n\n"
        text += "*Click a keyword button below to remove it.*"
    else:
        text += "No keywords configured yet. Messages won't be monitored. Use the button below to add your first keyword!"
    return text

def get_kw_keywords_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    keywords = get_keywords()
    
    for kw in keywords:
        safe_kw = kw[:40]
        markup.add(InlineKeyboardButton(f"❌ {safe_kw}", callback_data=f"kw_mon_del_key_{safe_kw}"))
        
    markup.add(InlineKeyboardButton("➕ Add Keyword", callback_data="kw_mon_add_key_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="keyword_mon_main"))
    return markup

def get_kw_groups_text():
    groups = get_keyword_monitored_groups()
    text = "👥 *Monitored Groups for Keywords & Links*\n\n"
    text += "Configure which groups/channels you want to monitor (excluding source chats of your Target Pairs, which are monitored automatically).\n\n"
    if groups:
        text += "*Click a group below to stop monitoring it:*"
    else:
        text += "_No groups explicitly monitored yet. Use the button below to add one._"
    return text

def get_kw_groups_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    groups = get_keyword_monitored_groups()
    
    for chat_id, title in groups:
        markup.add(InlineKeyboardButton(f"❌ {title}", callback_data=f"kw_grp_del_{chat_id}"))
        
    markup.add(InlineKeyboardButton("➕ Add Monitored Group", callback_data="kw_grp_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="keyword_mon_main"))
    return markup

# --- Custom Chat Selection for Alert Destination ---
async def get_keyword_dest_selection_markup(page=0):
    markup = InlineKeyboardMarkup(row_width=1)
    client = userbot_fleet_manager.get_any_client()
    if not client or not client.is_connected():
        return None
        
    chats = []
    async for dialog in client.iter_dialogs(limit=100):
        entity = dialog.entity
        if isinstance(entity, (types.Chat, types.Channel)):
            chats.append(dialog)
            
    # Pagination
    start = page * 10
    end = start + 10
    page_items = chats[start:end]
    
    current_dest = get_alert_destination()
    
    # 1. Option for Admin DM
    admin_selected = " ✅" if current_dest == ADMIN_ID else ""
    markup.add(InlineKeyboardButton(f"👤 Admin DM (Default){admin_selected}", callback_data="kw_dest_set_admin"))
    
    # 2. Add dialog items
    for dialog in page_items:
        chat = dialog.entity
        is_forum = getattr(chat, "forum", False)
        if isinstance(chat, types.Channel):
            if is_forum: icon = "🏛️"; title = f"『 TOPIC 』 {chat.title}"
            elif chat.broadcast: icon = "📢"; title = chat.title or "Channel"
            else: icon = "👥"; title = chat.title or "Group"
        elif isinstance(chat, types.Chat):
            icon = "👥"; title = chat.title or "Group"
        else:
            continue
            
        is_selected = False
        if current_dest == dialog.id or current_dest in get_alternate_ids(dialog.id) or dialog.id in get_alternate_ids(current_dest):
            is_selected = True
            
        selected_icon = " ✅" if is_selected else ""
        markup.add(
            InlineKeyboardButton(
                f"{icon} {title}{selected_icon}",
                callback_data=f"kw_dest_set_{dialog.id}_{page}"
            )
        )
        
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"kw_dest_page_{page-1}"))
    if end < len(chats):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"kw_dest_page_{page+1}"))
    if nav:
        markup.add(*nav)
        
    markup.add(InlineKeyboardButton("🔍 Search Chat", callback_data="kw_dest_search_start"))
    markup.add(InlineKeyboardButton("🔙 Done", callback_data="keyword_mon_main"))
    return markup

# --- Custom Chat Selection with Multi-Select ---
async def get_keyword_chat_selection_markup(page=0):
    markup = InlineKeyboardMarkup(row_width=1)
    client = userbot_fleet_manager.get_any_client()
    if not client or not client.is_connected():
        return None
    
    chats = []
    # Fetch enough dialogs
    async for dialog in client.iter_dialogs(limit=100):
        entity = dialog.entity
        if isinstance(entity, (types.Chat, types.Channel)):
            chats.append(dialog)
            
    # Pagination
    start = page * 10
    end = start + 10
    page_items = chats[start:end]
    
    monitored_grps = get_keyword_monitored_groups()
    monitored_ids = {int(g[0]) for g in monitored_grps}
    
    for dialog in page_items:
        chat = dialog.entity
        is_forum = getattr(chat, "forum", False)
        
        if isinstance(chat, types.Channel):
            if is_forum:
                icon = "🏛️"
                title = f"『 TOPIC 』 {chat.title}"
            elif chat.broadcast:
                icon = "📢"
                title = chat.title or "Channel"
            else:
                icon = "👥"
                title = chat.title or "Group"
        elif isinstance(chat, types.Chat):
            icon = "👥"
            title = chat.title or "Group"
        else:
            continue
            
        # Check selection using alternate IDs for backward compatibility
        is_selected = False
        for mid in monitored_ids:
            if dialog.id == mid or dialog.id in get_alternate_ids(mid) or mid in get_alternate_ids(dialog.id):
                is_selected = True
                break
                
        selected_icon = " ✅" if is_selected else ""
        
        markup.add(
            InlineKeyboardButton(
                f"{icon} {title}{selected_icon}",
                callback_data=f"kw_grp_toggle_{dialog.id}_{page}"
            )
        )
        
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"kw_grp_page_{page-1}"))
    if end < len(chats):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"kw_grp_page_{page+1}"))
    if nav:
        markup.add(*nav)
        
    markup.add(InlineKeyboardButton("🔍 Search Group", callback_data="kw_grp_search_start"))
    markup.add(InlineKeyboardButton("🔙 Done", callback_data="kw_mon_groups"))
    return markup

# --- Intercept Dashboard ---
original_get_dashboard_markup = main_module.get_dashboard_markup

def new_get_dashboard_markup():
    markup = original_get_dashboard_markup()
    clients = userbot_fleet_manager.get_all_clients()
    connected_clients = [c for c in clients if c.is_connected()]
    if connected_clients:
        has_btn = any(getattr(b, 'callback_data', '') == "kw_mon_main" for row in markup.keyboard for b in row)
        if not has_btn:
            markup.add(InlineKeyboardButton("🔑 Keyword Monitor", callback_data="kw_mon_main"))
    return markup

main_module.get_dashboard_markup = new_get_dashboard_markup

# --- Callback Handlers ---
@bot.callback_query_handler(func=lambda call: is_authorized_manager(call.from_user.id) and call.data.startswith("kw_"))
def handle_kw_plugin_callbacks(call):
    uid = call.from_user.id
    data = call.data
    
    if data == "kw_mon_main":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(get_keyword_monitor_text(), call.message.chat.id, call.message.message_id, reply_markup=get_keyword_monitor_markup(), parse_mode="Markdown")
        
    elif data == "kw_mon_toggle":
        active = get_setting("keyword_check_active", "0") == "1"
        new_state = "0" if active else "1"
        set_setting("keyword_check_active", new_state)
        bot.answer_callback_query(call.id, "Keyword check enabled" if new_state == "1" else "Keyword check disabled")
        bot.edit_message_text(get_keyword_monitor_text(), call.message.chat.id, call.message.message_id, reply_markup=get_keyword_monitor_markup(), parse_mode="Markdown")
        
    elif data == "kw_mon_toggle_links":
        active = get_setting("keyword_check_links_active", "0") == "1"
        new_state = "0" if active else "1"
        set_setting("keyword_check_links_active", new_state)
        bot.answer_callback_query(call.id, "Link monitor enabled" if new_state == "1" else "Link monitor disabled")
        bot.edit_message_text(get_keyword_monitor_text(), call.message.chat.id, call.message.message_id, reply_markup=get_keyword_monitor_markup(), parse_mode="Markdown")

    elif data == "kw_dest_manage":
        bot.answer_callback_query(call.id)
        async def show_dest_menu():
            markup = await get_keyword_dest_selection_markup(0)
            if markup:
                bot.edit_message_text(
                    "🎯 *Select Alert Destination*\n\nChoose where keyword/link match alerts should be sent (Admin DM by default):",
                    call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown"
                )
            else:
                bot.send_message(call.message.chat.id, "❌ Main Userbot Offline/Disconnected. Cannot fetch chat list.")
        asyncio.run_coroutine_threadsafe(show_dest_menu(), loop)

    elif data.startswith("kw_dest_page_"):
        bot.answer_callback_query(call.id)
        page = int(data.split("_")[3])
        async def show_page():
            markup = await get_keyword_dest_selection_markup(page)
            if markup:
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        asyncio.run_coroutine_threadsafe(show_page(), loop)

    elif data == "kw_dest_set_admin":
        bot.answer_callback_query(call.id, "Destination set to Admin DM")
        set_setting("keyword_check_alert_destination", "")
        async def refresh():
            markup = await get_keyword_dest_selection_markup(0)
            if markup:
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        asyncio.run_coroutine_threadsafe(refresh(), loop)

    elif data.startswith("kw_dest_set_"):
        parts = data.split("_")
        cid = int(parts[3])
        page = int(parts[4])
        bot.answer_callback_query(call.id, "Alert destination updated!")
        set_setting("keyword_check_alert_destination", str(cid))
        async def refresh():
            markup = await get_keyword_dest_selection_markup(page)
            if markup:
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        asyncio.run_coroutine_threadsafe(refresh(), loop)

    elif data == "kw_dest_search_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_kw_dest_search"
        bot.send_message(
            call.message.chat.id,
            "🔍 *Search Destination Chat*\n\nPlease send me the exact name or keyword of the group/channel where you want to route alerts:",
            parse_mode="Markdown"
        )

    elif data.startswith("kw_dest_search_toggle_"):
        parts = data.split("_")
        cid = int(parts[4])
        query = "_".join(parts[5:])
        
        bot.answer_callback_query(call.id, "Alert destination updated!")
        set_setting("keyword_check_alert_destination", str(cid))
        
        current_dest = get_alert_destination()
        new_keyboard = []
        for row in call.message.reply_markup.keyboard:
            new_row = []
            for btn in row:
                if btn.callback_data.startswith("kw_dest_search_toggle_"):
                    b_parts = btn.callback_data.split("_")
                    b_cid = int(b_parts[4])
                    current_text = btn.text
                    if current_text.endswith(" ✅"):
                        base_text = current_text[:-2]
                    else:
                        base_text = current_text
                        
                    btn_selected = False
                    if current_dest == b_cid or current_dest in get_alternate_ids(b_cid) or b_cid in get_alternate_ids(current_dest):
                        btn_selected = True
                        
                    if btn_selected:
                        new_text = f"{base_text} ✅"
                    else:
                        new_text = base_text
                    new_row.append(InlineKeyboardButton(new_text, callback_data=btn.callback_data))
                else:
                    new_row.append(btn)
            new_keyboard.append(new_row)
            
        reply_markup = InlineKeyboardMarkup(keyboard=new_keyboard)
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=reply_markup)

    elif data == "kw_mon_keywords":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(get_kw_keywords_text(), call.message.chat.id, call.message.message_id, reply_markup=get_kw_keywords_markup(), parse_mode="Markdown")

    elif data == "kw_mon_add_key_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_new_keyword"
        bot.send_message(call.message.chat.id, "➕ *Add Keyword*\n\nPlease send me the keyword or phrase you want to monitor (case-insensitive):")

    elif data == "kw_mon_groups":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(get_kw_groups_text(), call.message.chat.id, call.message.message_id, reply_markup=get_kw_groups_markup(), parse_mode="Markdown")

    elif data == "kw_grp_add_start":
        bot.answer_callback_query(call.id, "🔍 Loading your chats...")
        async def show_kw_grp_list():
            try:
                is_ok, msg = await ensure_userbot()
                if not is_ok:
                    bot.send_message(call.message.chat.id, f"❌ Userbot connection failed: {msg}\n\nPlease go to *👤 User Account* and ensure your session is active.")
                    return
                
                markup = await get_keyword_chat_selection_markup(0)
                if markup:
                    bot.edit_message_text("🎯 *Select Group/Channel*\nChoose the chats you want to monitor (marked with ✅):", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                else:
                    bot.edit_message_text("❌ No chats found. Make sure your userbot is in at least one group or channel.", call.message.chat.id, call.message.message_id, reply_markup=get_keyword_monitor_markup())
            except Exception as e:
                logger.error(f"Add Keyword Group Start Error: {e}")
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")
        asyncio.run_coroutine_threadsafe(show_kw_grp_list(), loop)

    elif data.startswith("kw_grp_page_"):
        bot.answer_callback_query(call.id)
        page = int(data.split("_")[3])
        async def update_kw_grp_page():
            markup = await get_keyword_chat_selection_markup(page)
            if markup:
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        asyncio.run_coroutine_threadsafe(update_kw_grp_page(), loop)

    elif data.startswith("kw_grp_toggle_"):
        parts = data.split("_")
        cid = int(parts[3])
        page = int(parts[4])
        
        # Toggle DB
        monitored_grps = get_keyword_monitored_groups()
        monitored_ids = {int(g[0]) for g in monitored_grps}
        
        # Check selection using alternate IDs compatibility
        is_selected = False
        matched_id = None
        for mid in monitored_ids:
            if cid == mid or cid in get_alternate_ids(mid) or mid in get_alternate_ids(cid):
                is_selected = True
                matched_id = mid
                break
                
        if is_selected:
            remove_keyword_monitored_group(matched_id)
            bot.answer_callback_query(call.id, "Removed from monitor")
        else:
            title = ""
            for row in call.message.reply_markup.keyboard:
                for btn in row:
                    if btn.callback_data == data:
                        title = btn.text
                        if title.endswith(" ✅"):
                            title = title[:-2]
                        if " " in title:
                            title = title.split(" ", 1)[1]
                        break
            if not title:
                title = f"Chat {cid}"
            add_keyword_monitored_group(cid, title)
            bot.answer_callback_query(call.id, f"Added: {title}")
            
        # Update checked state in UI instantly
        monitored_grps = get_keyword_monitored_groups()
        monitored_ids = {int(g[0]) for g in monitored_grps}
        
        new_keyboard = []
        for row in call.message.reply_markup.keyboard:
            new_row = []
            for btn in row:
                if btn.callback_data.startswith("kw_grp_toggle_"):
                    b_parts = btn.callback_data.split("_")
                    b_cid = int(b_parts[3])
                    current_text = btn.text
                    if current_text.endswith(" ✅"):
                        base_text = current_text[:-2]
                    else:
                        base_text = current_text
                        
                    # Check selection using alternate IDs compatibility
                    btn_selected = False
                    for mid in monitored_ids:
                        if b_cid == mid or b_cid in get_alternate_ids(mid) or mid in get_alternate_ids(b_cid):
                            btn_selected = True
                            break
                            
                    if btn_selected:
                        new_text = f"{base_text} ✅"
                    else:
                        new_text = base_text
                    new_row.append(InlineKeyboardButton(new_text, callback_data=btn.callback_data))
                else:
                    new_row.append(btn)
            new_keyboard.append(new_row)
            
        reply_markup = InlineKeyboardMarkup(keyboard=new_keyboard)
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=reply_markup)

    elif data.startswith("kw_mon_del_key_"):
        bot.answer_callback_query(call.id)
        word = data.replace("kw_mon_del_key_", "")
        remove_keyword(word)
        bot.answer_callback_query(call.id, f"Removed keyword: {word}")
        bot.edit_message_text(get_kw_keywords_text(), call.message.chat.id, call.message.message_id, reply_markup=get_kw_keywords_markup(), parse_mode="Markdown")

    elif data.startswith("kw_grp_del_"):
        bot.answer_callback_query(call.id)
        cid = int(data.replace("kw_grp_del_", ""))
        remove_keyword_monitored_group(cid)
        bot.answer_callback_query(call.id, "Group removed from monitor.")
        bot.edit_message_text(get_kw_groups_text(), call.message.chat.id, call.message.message_id, reply_markup=get_kw_groups_markup(), parse_mode="Markdown")

    elif data == "kw_grp_search_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_kw_grp_search"
        bot.send_message(
            call.message.chat.id,
            "🔍 *Search Group or Channel*\n\nPlease send me the name or keyword of the group/channel you want to search:"
        )

    elif data.startswith("kw_grp_search_toggle_"):
        parts = data.split("_")
        cid = int(parts[4])
        query = "_".join(parts[5:])
        
        # Toggle DB
        monitored_grps = get_keyword_monitored_groups()
        monitored_ids = {int(g[0]) for g in monitored_grps}
        
        # Check selection using alternate IDs compatibility
        is_selected = False
        matched_id = None
        for mid in monitored_ids:
            if cid == mid or cid in get_alternate_ids(mid) or mid in get_alternate_ids(cid):
                is_selected = True
                matched_id = mid
                break
                
        if is_selected:
            remove_keyword_monitored_group(matched_id)
            bot.answer_callback_query(call.id, "Removed from monitor")
        else:
            title = ""
            for row in call.message.reply_markup.keyboard:
                for btn in row:
                    if btn.callback_data == data:
                        title = btn.text
                        if title.endswith(" ✅"):
                            title = title[:-2]
                        if " " in title:
                            title = title.split(" ", 1)[1]
                        break
            if not title:
                title = f"Chat {cid}"
            add_keyword_monitored_group(cid, title)
            bot.answer_callback_query(call.id, f"Added: {title}")
            
        # Update checked state in search UI instantly
        monitored_grps = get_keyword_monitored_groups()
        monitored_ids = {int(g[0]) for g in monitored_grps}
        
        new_keyboard = []
        for row in call.message.reply_markup.keyboard:
            new_row = []
            for btn in row:
                if btn.callback_data.startswith("kw_grp_search_toggle_"):
                    b_parts = btn.callback_data.split("_")
                    b_cid = int(b_parts[4])
                    current_text = btn.text
                    if current_text.endswith(" ✅"):
                        base_text = current_text[:-2]
                    else:
                        base_text = current_text
                        
                    # Check selection using alternate IDs compatibility
                    btn_selected = False
                    for mid in monitored_ids:
                        if b_cid == mid or b_cid in get_alternate_ids(mid) or mid in get_alternate_ids(b_cid):
                            btn_selected = True
                            break
                            
                    if btn_selected:
                        new_text = f"{base_text} ✅"
                    else:
                        new_text = base_text
                    new_row.append(InlineKeyboardButton(new_text, callback_data=btn.callback_data))
                else:
                    new_row.append(btn)
            new_keyboard.append(new_row)
            
        reply_markup = InlineKeyboardMarkup(keyboard=new_keyboard)
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=reply_markup)

# --- Command Handlers ---
@bot.message_handler(commands=['addkey'])
def cmd_add_key(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            bot.reply_to(message, "💡 *Usage:* `/addkey [keyword]`", parse_mode="Markdown")
            return
        word = args[1]
        add_keyword(word)
        bot.reply_to(message, f"✅ *Keyword Added:* `{word.strip().lower()}`", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['delkey', 'removekey'])
def cmd_del_key(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            bot.reply_to(message, "💡 *Usage:* `/delkey [keyword]`", parse_mode="Markdown")
            return
        word = args[1]
        remove_keyword(word)
        bot.reply_to(message, f"✅ *Keyword Removed:* `{word.strip().lower()}`", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['changekey'])
def cmd_change_key(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 3:
            bot.reply_to(message, "💡 *Usage:* `/changekey [old_keyword] [new_keyword]`", parse_mode="Markdown")
            return
        old_word = args[1].strip().lower()
        new_word = args[2].strip().lower()
        
        keys = get_keywords()
        if old_word not in keys:
            bot.reply_to(message, f"❌ *Keyword not found:* `{old_word}`", parse_mode="Markdown")
            return
            
        remove_keyword(old_word)
        add_keyword(new_word)
        bot.reply_to(message, f"✅ *Keyword Changed:* `{old_word}` ➡️ `{new_word}`", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['keywords', 'listkeys', 'allkeys'])
def cmd_list_keys(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        keys = get_keywords()
        active = get_setting("keyword_check_active", "0") == "1"
        status = "🟢 *Active*" if active else "🔴 *Inactive*"
        
        if not keys:
            bot.reply_to(message, f"📋 *Keyword List* (Status: {status})\n\nNo keywords configured yet. Use `/addkey` to add one.", parse_mode="Markdown")
            return
            
        keys_str = "\n".join(f"- `{k}`" for k in keys)
        bot.reply_to(message, f"📋 *Keyword List* (Status: {status})\n\n{keys_str}", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['monitor'])
def cmd_monitor_group(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            bot.reply_to(message, "💡 *Usage:* `/monitor [link_or_username_or_id]`", parse_mode="Markdown")
            return
        target = args[1].strip()
        
        async def do_resolve():
            resolve_target_across_fleet = getattr(main_module, 'resolve_target_across_fleet', None)
            if not resolve_target_across_fleet:
                client = userbot_fleet_manager.get_any_client()
                if not client:
                    bot.reply_to(message, "❌ No active userbots to resolve the chat.")
                    return
                try:
                    entity = await resolve_target_id(client, target)
                except Exception as e:
                    bot.reply_to(message, f"❌ Could not resolve target: {e}")
                    return
            else:
                client, entity = await resolve_target_across_fleet(target)
                if not entity:
                    bot.reply_to(message, f"❌ Could not resolve target: {target}")
                    return
            
            from telethon.utils import get_peer_id
            cid = get_peer_id(entity)
            title = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(cid)
            
            add_keyword_monitored_group(cid, title)
            bot.reply_to(message, f"✅ *Monitored Group Added:* `{title}` (ID: `{cid}`)", parse_mode="Markdown")
            
        asyncio.run_coroutine_threadsafe(do_resolve(), loop)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['unmonitor'])
def cmd_unmonitor_group(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            bot.reply_to(message, "💡 *Usage:* `/unmonitor [link_or_username_or_id_or_title]`", parse_mode="Markdown")
            return
        target = args[1].strip()
        
        resolved_cid = None
        try:
            resolved_cid = int(target)
        except ValueError:
            pass
            
        if resolved_cid:
            monitored = get_keyword_monitored_groups()
            found = False
            for cid, title in monitored:
                if cid == resolved_cid or cid in get_alternate_ids(resolved_cid) or resolved_cid in get_alternate_ids(cid):
                    remove_keyword_monitored_group(cid)
                    bot.reply_to(message, f"✅ *Removed from monitored groups:* `{title}` (ID: `{cid}`)", parse_mode="Markdown")
                    found = True
                    break
            if not found:
                bot.reply_to(message, f"❌ ID `{resolved_cid}` is not in monitored groups.", parse_mode="Markdown")
            return
            
        async def do_unresolve():
            resolve_target_across_fleet = getattr(main_module, 'resolve_target_across_fleet', None)
            client = None
            entity = None
            if resolve_target_across_fleet:
                client, entity = await resolve_target_across_fleet(target)
            
            if not entity:
                client = userbot_fleet_manager.get_any_client()
                if client:
                    try:
                        entity = await resolve_target_id(client, target)
                    except Exception:
                        pass
            
            if entity:
                from telethon.utils import get_peer_id
                cid = get_peer_id(entity)
                remove_keyword_monitored_group(cid)
                title = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(cid)
                bot.reply_to(message, f"✅ *Removed from monitored groups:* `{title}` (ID: `{cid}`)", parse_mode="Markdown")
                return
                
            monitored = get_keyword_monitored_groups()
            for cid, title in monitored:
                if target.lower() in title.lower():
                    remove_keyword_monitored_group(cid)
                    bot.reply_to(message, f"✅ *Removed from monitored groups:* `{title}` (ID: `{cid}`)", parse_mode="Markdown")
                    return
                    
            bot.reply_to(message, f"❌ Could not resolve target or match title: `{target}`", parse_mode="Markdown")
            
        asyncio.run_coroutine_threadsafe(do_unresolve(), loop)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['monitoredgroups', 'listgroups', 'monitored'])
def cmd_list_groups(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        grps = get_keyword_monitored_groups()
        if not grps:
            bot.reply_to(message, "📋 *Monitored Groups:* None configured.", parse_mode="Markdown")
            return
        lines = []
        for cid, title in grps:
            lines.append(f"- `{title}` (ID: `{cid}`)")
        bot.reply_to(message, "📋 *Monitored Groups:*\n\n" + "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['keycheck'])
def cmd_keycheck_toggle(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        args = message.text.split()
        if len(args) < 2 or args[1].lower() not in ["on", "off"]:
            bot.reply_to(message, "💡 *Usage:* `/keycheck [on/off]`", parse_mode="Markdown")
            return
        
        state = args[1].lower()
        active = "1" if state == "on" else "0"
        set_setting("keyword_check_active", active)
        status_lbl = "🟢 *Enabled*" if state == "on" else "🔴 *Disabled*"
        bot.reply_to(message, f"🔔 *Keyword Check status:* {status_lbl}", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['linkcheck'])
def cmd_linkcheck_toggle(message):
    if not is_authorized_manager(message.from_user.id): return
    try:
        args = message.text.split()
        if len(args) < 2 or args[1].lower() not in ["on", "off"]:
            bot.reply_to(message, "💡 *Usage:* `/linkcheck [on/off]`", parse_mode="Markdown")
            return
        
        state = args[1].lower()
        active = "1" if state == "on" else "0"
        set_setting("keyword_check_links_active", active)
        status_lbl = "🟢 *Enabled*" if state == "on" else "🔴 *Disabled*"
        bot.reply_to(message, f"🔗 *Link Check status:* {status_lbl}", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# --- Input State Handler for Search & Keywords ---
@bot.message_handler(func=lambda m: is_authorized_manager(m.from_user.id) and admin_states.get(m.from_user.id) in ["awaiting_new_keyword", "awaiting_kw_grp_search", "awaiting_kw_dest_search"])
def handle_plugin_state_inputs(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    text = message.text.strip() if message.text else ""
    
    if state == "awaiting_new_keyword":
        if text:
            add_keyword(text)
            admin_states.pop(uid, None)
            bot.reply_to(message, f"✅ *Keyword Added:* `{text.lower()}`", parse_mode="Markdown")
            bot.send_message(message.chat.id, get_kw_keywords_text(), reply_markup=get_kw_keywords_markup(), parse_mode="Markdown")
        else:
            bot.reply_to(message, "❌ Invalid input. Keyword cannot be empty.")
            
    elif state == "awaiting_kw_grp_search":
        if not text:
            bot.reply_to(message, "❌ Invalid input.")
            return
        admin_states.pop(uid, None)
        
        async def run_search():
            search_msg = bot.send_message(message.chat.id, "🔍 Searching dialogues...")
            try:
                client = userbot_fleet_manager.get_any_client()
                chats = []
                async for dialog in client.iter_dialogs(limit=500):
                    entity = dialog.entity
                    if isinstance(entity, (types.Chat, types.Channel)):
                        title = entity.title or ""
                        if text.lower() in title.lower():
                            chats.append(dialog)
                            
                bot.delete_message(message.chat.id, search_msg.message_id)
                if not chats:
                    bot.send_message(message.chat.id, f"❌ No group or channel found matching `{text}`.", parse_mode="Markdown")
                    return
                    
                markup = InlineKeyboardMarkup(row_width=1)
                monitored_grps = get_keyword_monitored_groups()
                monitored_ids = {int(g[0]) for g in monitored_grps}
                
                for dialog in chats[:15]:
                    chat = dialog.entity
                    is_forum = getattr(chat, "forum", False)
                    if isinstance(chat, types.Channel):
                        if is_forum: icon = "🏛️"; t = f"『 TOPIC 』 {chat.title}"
                        elif chat.broadcast: icon = "📢"; t = chat.title
                        else: icon = "👥"; t = chat.title
                    elif isinstance(chat, types.Chat):
                        icon = "👥"; t = chat.title
                    else:
                        continue
                        
                    # Check selection using alternate IDs compatibility
                    is_selected = False
                    for mid in monitored_ids:
                        if dialog.id == mid or dialog.id in get_alternate_ids(mid) or mid in get_alternate_ids(dialog.id):
                            is_selected = True
                            break
                            
                    selected_icon = " ✅" if is_selected else ""
                    
                    markup.add(
                        InlineKeyboardButton(
                            f"{icon} {t}{selected_icon}",
                            callback_data=f"kw_grp_search_toggle_{dialog.id}_{text}"
                        )
                    )
                    
                markup.add(InlineKeyboardButton("🔙 Done", callback_data="kw_mon_groups"))
                bot.send_message(message.chat.id, f"🔍 *Search Results for '{text}':*", reply_markup=markup, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Error in run_search: {e}")
                bot.send_message(message.chat.id, f"❌ Error searching: {e}")
                
        asyncio.run_coroutine_threadsafe(run_search(), loop)

    elif state == "awaiting_kw_dest_search":
        if not text:
            bot.reply_to(message, "❌ Invalid input.")
            return
        admin_states.pop(uid, None)
        
        async def run_dest_search():
            search_msg = bot.send_message(message.chat.id, "🔍 Searching dialogues...")
            try:
                client = userbot_fleet_manager.get_any_client()
                chats = []
                async for dialog in client.iter_dialogs(limit=500):
                    entity = dialog.entity
                    if isinstance(entity, (types.Chat, types.Channel)):
                        title = entity.title or ""
                        if text.lower() in title.lower():
                            chats.append(dialog)
                            
                bot.delete_message(message.chat.id, search_msg.message_id)
                if not chats:
                    bot.send_message(message.chat.id, f"❌ No group or channel found matching `{text}`.", parse_mode="Markdown")
                    return
                    
                markup = InlineKeyboardMarkup(row_width=1)
                current_dest = get_alert_destination()
                
                for dialog in chats[:15]:
                    chat = dialog.entity
                    is_forum = getattr(chat, "forum", False)
                    if isinstance(chat, types.Channel):
                        if is_forum: icon = "🏛️"; t = f"『 TOPIC 』 {chat.title}"
                        elif chat.broadcast: icon = "📢"; t = chat.title
                        else: icon = "👥"; t = chat.title
                    elif isinstance(chat, types.Chat):
                        icon = "👥"; t = chat.title
                    else:
                        continue
                        
                    is_selected = False
                    if current_dest == dialog.id or current_dest in get_alternate_ids(dialog.id) or dialog.id in get_alternate_ids(current_dest):
                        is_selected = True
                    selected_icon = " ✅" if is_selected else ""
                    
                    markup.add(
                        InlineKeyboardButton(
                            f"{icon} {t}{selected_icon}",
                            callback_data=f"kw_dest_search_toggle_{dialog.id}_{text}"
                        )
                    )
                    
                markup.add(InlineKeyboardButton("🔙 Done", callback_data="kw_dest_manage"))
                bot.send_message(message.chat.id, f"🔍 *Search Results for '{text}':*", reply_markup=markup, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Error in run_dest_search: {e}")
                bot.send_message(message.chat.id, f"❌ Error searching: {e}")
                
        asyncio.run_coroutine_threadsafe(run_dest_search(), loop)

# --- Hook into Userbot Clients ---
def setup_keyword_monitor_handlers(client):
    if getattr(client, '_keyword_monitor_registered', False):
        return
    client._keyword_monitor_registered = True
    
    @client.on(events.NewMessage(incoming=True))
    async def plugin_keyword_handler(event):
        m = event.message
        if not m: return
        
        # Ensure client._me is cached
        if not hasattr(client, '_me') or not client._me:
            try:
                client._me = await client.get_me()
            except Exception as e:
                logger.error(f"Failed to get_me() for userbot in plugin: {e}")
                
        me = getattr(client, '_me', None)
        
        # Ignore messages sent by any userbot in the fleet to prevent loops/duplicate alerts
        fleet_user_ids = {c._me.id for c in userbot_fleet_manager.get_all_clients() if getattr(c, '_me', None)}
        if me:
            fleet_user_ids.add(me.id)
            
        if event.is_private and me and m.sender_id not in fleet_user_ids:
            is_primary_admin = (m.sender_id == ADMIN_ID) or (me and m.sender_id == me.id)
            is_manager = is_primary_admin or is_authorized_manager(m.sender_id)
            if not is_manager:
                return
                
            text = m.text.strip() if m.text else ""
            if not text.startswith('.'):
                return
                
            parts = text.split(None, 1)
            cmd = parts[0].lower()
            
            if cmd == '.addkey':
                if len(parts) < 2:
                    await event.reply("❌ **Usage:** `.addkey <keyword>`")
                    return
                word = parts[1].strip()
                add_keyword(word)
                await event.reply(f"✅ **Keyword Added:** `{word.lower()}`")
                return
                
            elif cmd in ['.delkey', '.removekey']:
                if len(parts) < 2:
                    await event.reply("❌ **Usage:** `.delkey <keyword>`")
                    return
                word = parts[1].strip()
                remove_keyword(word)
                await event.reply(f"✅ **Keyword Removed:** `{word.lower()}`")
                return
                
            elif cmd == '.changekey':
                subparts = parts[1].split(None, 1) if len(parts) > 1 else []
                if len(subparts) < 2:
                    await event.reply("❌ **Usage:** `.changekey <old_keyword> <new_keyword>`")
                    return
                old_word = subparts[0].strip().lower()
                new_word = subparts[1].strip().lower()
                
                keys = get_keywords()
                if old_word not in keys:
                    await event.reply(f"❌ **Keyword not found:** `{old_word}`")
                    return
                    
                remove_keyword(old_word)
                add_keyword(new_word)
                await event.reply(f"✅ **Keyword Changed:** `{old_word}` ➡️ `{new_word}`")
                return
                
            elif cmd in ['.allkeys', '.listkeys']:
                keys = get_keywords()
                active = get_setting("keyword_check_active", "0") == "1"
                status = "🟢 **Active**" if active else "🔴 **Inactive**"
                if not keys:
                    await event.reply(f"📋 **Keyword List** (Status: {status})\n\nNo keywords configured yet.")
                    return
                keys_str = "\n".join(f"- `{k}`" for k in keys)
                await event.reply(f"📋 **Keyword List** (Status: {status})\n\n{keys_str}")
                return
                
            elif cmd == '.monitor':
                if len(parts) < 2:
                    await event.reply("❌ **Usage:** `.monitor <link_or_username_or_id>`")
                    return
                target = parts[1].strip()
                await event.reply("⏳ **Resolving target chat...**")
                
                try:
                    resolve_target_across_fleet = getattr(main_module, 'resolve_target_across_fleet', None)
                    if resolve_target_across_fleet:
                        client_found, entity = await resolve_target_across_fleet(target)
                    else:
                        entity = await resolve_target_id(client, target)
                        
                    if not entity:
                        await event.reply(f"❌ Could not resolve target: `{target}`")
                        return
                        
                    from telethon.utils import get_peer_id
                    cid = get_peer_id(entity)
                    title = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(cid)
                    
                    add_keyword_monitored_group(cid, title)
                    await event.reply(f"✅ **Monitored Group Added:** `{title}` (ID: `{cid}`)")
                except Exception as e:
                    await event.reply(f"❌ **Error:** {e}")
                return
                
            elif cmd == '.unmonitor':
                if len(parts) < 2:
                    await event.reply("❌ **Usage:** `.unmonitor <link_or_username_or_id_or_title>`")
                    return
                target = parts[1].strip()
                
                resolved_cid = None
                try:
                    resolved_cid = int(target)
                except ValueError:
                    pass
                    
                if resolved_cid:
                    monitored = get_keyword_monitored_groups()
                    found = False
                    for cid, title in monitored:
                        if cid == resolved_cid or cid in get_alternate_ids(resolved_cid) or resolved_cid in get_alternate_ids(cid):
                            remove_keyword_monitored_group(cid)
                            await event.reply(f"✅ **Removed from monitored groups:** `{title}` (ID: `{cid}`)")
                            found = True
                            break
                    if not found:
                        await event.reply(f"❌ ID `{resolved_cid}` is not in monitored groups.")
                    return
                    
                try:
                    resolve_target_across_fleet = getattr(main_module, 'resolve_target_across_fleet', None)
                    entity = None
                    if resolve_target_across_fleet:
                        _, entity = await resolve_target_across_fleet(target)
                    else:
                        entity = await resolve_target_id(client, target)
                except Exception:
                    entity = None
                    
                if entity:
                    from telethon.utils import get_peer_id
                    cid = get_peer_id(entity)
                    remove_keyword_monitored_group(cid)
                    title = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(cid)
                    await event.reply(f"✅ **Removed from monitored groups:** `{title}` (ID: `{cid}`)")
                    return
                    
                monitored = get_keyword_monitored_groups()
                for cid, title in monitored:
                    if target.lower() in title.lower():
                        remove_keyword_monitored_group(cid)
                        await event.reply(f"✅ **Removed from monitored groups:** `{title}` (ID: `{cid}`)")
                        return
                        
                await event.reply(f"❌ Could not resolve target or match title: `{target}`")
                return
                
            elif cmd in ['.monitoredgroups', '.listgroups', '.monitored']:
                grps = get_keyword_monitored_groups()
                if not grps:
                    await event.reply("📋 **Monitored Groups:** None configured.")
                    return
                lines = []
                for cid, title in grps:
                    lines.append(f"- `{title}` (ID: `{cid}`)")
                await event.reply("📋 **Monitored Groups:**\n\n" + "\n".join(lines))
                return

        if not event.is_private and me and m.sender_id not in fleet_user_ids:
            try:
                # Check if sender is banned
                sender_id = m.sender_id
                sender_username = getattr(m.sender, 'username', None)
                if is_user_banned and is_user_banned(sender_id, sender_username):
                    return
                
                kw_active = get_setting("keyword_check_active", "0") == "1"
                links_active = get_setting("keyword_check_links_active", "0") == "1"
                
                if (kw_active or links_active) and (m.text or m.message):
                    current_chat_id = event.chat_id
                    
                    configured_pairs = get_target_pairs()
                    allowed_chat_ids = {int(p[1]) for p in configured_pairs if p[1] is not None}
                    
                    monitored_grps = get_keyword_monitored_groups()
                    for g in monitored_grps:
                        allowed_chat_ids.add(int(g[0]))
                        
                    # Check compatibility of current chat ID with allowed chat IDs (both positive and negative forms)
                    chat_allowed = False
                    for allowed_id in allowed_chat_ids:
                        if current_chat_id == allowed_id or current_chat_id in get_alternate_ids(allowed_id) or allowed_id in get_alternate_ids(current_chat_id):
                            chat_allowed = True
                            break
                            
                    if chat_allowed:
                        text_content = m.text or m.message or ""
                        text_lower = text_content.lower()
                        
                        matched_keyword = None
                        matched_links = []
                        
                        # 1. Keyword Check
                        if kw_active and monitored_keywords:
                            for kw in monitored_keywords:
                                if kw in text_lower:
                                    matched_keyword = kw
                                    break
                                    
                        # 2. Link Check
                        if links_active:
                            extracted_links = []
                            # Check formatting entities
                            if m.entities:
                                for ent in m.entities:
                                    if isinstance(ent, types.MessageEntityUrl):
                                        offset = ent.offset
                                        length = ent.length
                                        url = text_content[offset:offset+length]
                                        extracted_links.append(url)
                                    elif isinstance(ent, types.MessageEntityTextUrl):
                                        extracted_links.append(ent.url)
                            # Regex fallback
                            urls = re.findall(r'(https?://[^\s]+|t\.me/[^\s]+)', text_content)
                            for u in urls:
                                if u not in extracted_links:
                                    extracted_links.append(u)
                            if extracted_links:
                                matched_links = extracted_links
                                
                        # Trigger alert if keyword or link matched
                        if matched_keyword or matched_links:
                            # Rate limiting check (30-second delay per chat & match item)
                            import time
                            current_time = time.time()
                            limit_key = (m.chat_id, matched_keyword or "link_match")
                            last_alert_time = keyword_alert_history.get(limit_key, 0)
                            if current_time - last_alert_time < 30:
                                logger.info(f"KEYWORD MONITOR: Suppressed spam alert in chat {m.chat_id} (rate limit active).")
                                return
                                
                            msg_key = f"kw_{m.chat_id}_{m.id}"
                            if msg_key not in processed_messages:
                                processed_messages.add(msg_key)
                                # Update last alert time
                                keyword_alert_history[limit_key] = current_time
                                
                                sender = await event.get_sender()
                                user_id = m.sender_id or "Unknown"
                                name = "Unknown Sender"
                                username = None
                                bio = "No Bio"
                                
                                if sender:
                                    user_id = sender.id
                                    if hasattr(sender, 'first_name'):
                                        first_name = getattr(sender, 'first_name', '') or ''
                                        last_name = getattr(sender, 'last_name', '') or ''
                                        name = f"{first_name} {last_name}".strip() or "User"
                                        username = getattr(sender, 'username', None)
                                        
                                        try:
                                            from telethon.tl.functions.users import GetFullUserRequest
                                            full_user = await client(GetFullUserRequest(user_id))
                                            bio = getattr(full_user.full_user, 'about', '') or "No Bio"
                                        except Exception as be:
                                            logger.error(f"Failed to fetch user bio in keyword check in plugin: {be}")
                                    else:
                                        name = getattr(sender, 'title', 'Channel/Chat')
                                        username = getattr(sender, 'username', None)
                                        bio = "N/A (Channel/Chat)"
                                        
                                if username:
                                    dm_link = f"https://t.me/{username}"
                                else:
                                    dm_link = f"tg://user?id={user_id}"
                                    
                                chat_title = "Group"
                                try:
                                    chat = await event.get_chat()
                                    chat_title = getattr(chat, 'title', 'Group')
                                except Exception:
                                    pass
                                    
                                # Construct Alert Message
                                if matched_keyword and matched_links:
                                    alert_msg = (
                                        f"🔑🚨 **Keyword & Link Match Alert!**\n\n"
                                        f"💬 **Group:** `{chat_title}` (ID: `{m.chat_id}`)\n"
                                        f"👤 **User:** `{name}`\n"
                                        f"🆔 **User ID:** `{user_id}`\n"
                                        f"👤 **Username:** " + (f"@{username}" if username else "None") + "\n"
                                        f"📝 **User Bio:** {bio}\n"
                                        f"🎯 **Matched Keyword:** `{matched_keyword}`\n"
                                        f"🔗 **Detected Links:** {', '.join(f'`{l}`' for l in matched_links)}\n\n"
                                        f"✉️ **Message:**\n\"{text_content}\"\n\n"
                                        f"🔗 **DM Link:** {dm_link}"
                                    )
                                elif matched_keyword:
                                    alert_msg = (
                                        f"🔑 **Keyword Match Alert!**\n\n"
                                        f"💬 **Group:** `{chat_title}` (ID: `{m.chat_id}`)\n"
                                        f"👤 **User:** `{name}`\n"
                                        f"🆔 **User ID:** `{user_id}`\n"
                                        f"👤 **Username:** " + (f"@{username}" if username else "None") + "\n"
                                        f"📝 **User Bio:** {bio}\n"
                                        f"🎯 **Matched Keyword:** `{matched_keyword}`\n\n"
                                        f"✉️ **Message:**\n\"{text_content}\"\n\n"
                                        f"🔗 **DM Link:** {dm_link}"
                                    )
                                else:
                                    alert_msg = (
                                        f"🔗 **Link Match Alert!**\n\n"
                                        f"💬 **Group:** `{chat_title}` (ID: `{m.chat_id}`)\n"
                                        f"👤 **User:** `{name}`\n"
                                        f"🆔 **User ID:** `{user_id}`\n"
                                        f"👤 **Username:** " + (f"@{username}" if username else "None") + "\n"
                                        f"📝 **User Bio:** {bio}\n"
                                        f"🎯 **Detected Links:** {', '.join(f'`{l}`' for l in matched_links)}\n\n"
                                        f"✉️ **Message:**\n\"{text_content}\"\n\n"
                                        f"🔗 **DM Link:** {dm_link}"
                                    )
                                    
                                alert_dest = get_alert_destination()
                                try:
                                    bot.send_message(alert_dest, alert_msg, parse_mode="Markdown")
                                except Exception as notify_err:
                                    logger.error(f"Failed to send keyword alert via bot: {notify_err}")
                                    try:
                                        await client.send_message(alert_dest, alert_msg)
                                    except Exception as se:
                                        logger.error(f"Failed to send keyword alert to destination: {se}")
            except Exception as ke:
                logger.error(f"Error in keyword checker logic in plugin: {ke}")

# Monkeypatch setup_automation_handlers to register our keyword handler
original_setup_automation_handlers = main_module.setup_automation_handlers

def new_setup_automation_handlers(client):
    original_setup_automation_handlers(client)
    setup_keyword_monitor_handlers(client)

main_module.setup_automation_handlers = new_setup_automation_handlers

# Apply to any already connected clients
for client in userbot_fleet_manager.get_all_clients():
    if client.is_connected():
        setup_keyword_monitor_handlers(client)
