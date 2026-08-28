import sys
import os
import json
import asyncio
import logging
import random
import time
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
import re

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
get_dashboard_markup = main_module.get_dashboard_markup
is_authorized_manager = main_module.is_authorized_manager
set_setting = main_module.set_setting
get_setting = main_module.get_setting
admin_states = main_module.admin_states
userbot_fleet_manager = main_module.userbot_fleet_manager
loop = main_module.loop
ADMIN_ID = main_module.ADMIN_ID


# Media folder configuration
MEDIA_DIR = "mailer_media"
os.makedirs(MEDIA_DIR, exist_ok=True)

# In-memory cache for userbot groups
userbot_groups_cache = {}

# Track currently running task broadcasts
active_broadcasts = {}

# Initialize database schema for task-based mailer
def init_plugin_db():
    try:
        with main_module.db_conn() as conn:
            c = conn.cursor()
            is_pg = main_module.USING_POSTGRES
            auto_inc = "SERIAL PRIMARY KEY" if is_pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
            
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS gm_tasks (
                    id {auto_inc},
                    name TEXT,
                    userbot_ids TEXT,
                    group_ids TEXT,
                    message TEXT,
                    repeat_interval INTEGER DEFAULT 0,
                    last_run REAL DEFAULT 0,
                    update_group_id TEXT
                )
            """)
            
            # Map join links schema inside plugin db
            c.execute("""
                CREATE TABLE IF NOT EXISTS gm_links_map (
                    group_id TEXT PRIMARY KEY,
                    link TEXT
                )
            """)
            
            # Map duplicate protection sent media history
            c.execute("""
                CREATE TABLE IF NOT EXISTS gm_sent_media_history (
                    task_id INTEGER,
                    source_chat_id TEXT,
                    message_id INTEGER,
                    PRIMARY KEY (task_id, source_chat_id, message_id)
                )
            """)
            conn.commit()

            # Add migration columns for media forwarding
            columns_to_add = [
                ("media_sources", "TEXT DEFAULT '[]'"),
                ("media_interval", "INTEGER DEFAULT 10"),
                ("media_mix", "INTEGER DEFAULT 0"),
                ("media_enabled", "INTEGER DEFAULT 0"),
                ("last_source_ids", "TEXT DEFAULT '{}'"),
                ("media_dedup", "INTEGER DEFAULT 0")
            ]
            for col_name, col_def in columns_to_add:
                try:
                    c.execute(f"ALTER TABLE gm_tasks ADD COLUMN {col_name} {col_def}")
                    conn.commit()
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Failed to initialize Group Mailer Task database: {e}")

# Run DB initialization
init_plugin_db()

# DB Helpers for Tasks
def db_create_task(name):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        if main_module.USING_POSTGRES:
            c.execute(
                "INSERT INTO gm_tasks (name, userbot_ids, group_ids, message, repeat_interval, last_run, update_group_id) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (name, "[]", "[]", "{}", 0, 0.0, "")
            )
            inserted_id = c.fetchone()[0]
            conn.commit()
            return inserted_id
        else:
            c.execute(
                "INSERT INTO gm_tasks (name, userbot_ids, group_ids, message, repeat_interval, last_run, update_group_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, "[]", "[]", "{}", 0, 0.0, "")
            )
            conn.commit()
            return c.lastrowid

def db_get_tasks():
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, name, repeat_interval, last_run FROM gm_tasks ORDER BY id DESC")
        return c.fetchall()

def db_get_task(task_id):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        fields = "id, name, userbot_ids, group_ids, message, repeat_interval, last_run, update_group_id, media_sources, media_interval, media_mix, media_enabled, last_source_ids, media_dedup"
        query = f"SELECT {fields} FROM gm_tasks WHERE id = ?" if not main_module.USING_POSTGRES else f"SELECT {fields} FROM gm_tasks WHERE id = %s"
        c.execute(query, (task_id,))
        return c.fetchone()

def db_update_task(task_id, field, value):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        query = f"UPDATE gm_tasks SET {field} = ? WHERE id = ?" if not main_module.USING_POSTGRES else f"UPDATE gm_tasks SET {field} = %s WHERE id = %s"
        c.execute(query, (value, task_id))
        conn.commit()

def db_delete_task(task_id):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM gm_tasks WHERE id = ?", (task_id,)) if not main_module.USING_POSTGRES else c.execute("DELETE FROM gm_tasks WHERE id = %s", (task_id,))
        c.execute("DELETE FROM gm_sent_media_history WHERE task_id = ?", (task_id,)) if not main_module.USING_POSTGRES else c.execute("DELETE FROM gm_sent_media_history WHERE task_id = %s", (task_id,))
        conn.commit()

# DB Helpers for Join Links
def db_save_link(group_id, link):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO gm_links_map (group_id, link) VALUES (?, ?)", (str(group_id), link)) if not main_module.USING_POSTGRES else c.execute("INSERT INTO gm_links_map (group_id, link) VALUES (%s, %s) ON CONFLICT (group_id) DO UPDATE SET link = EXCLUDED.link", (str(group_id), link))
        conn.commit()

def db_get_links_map():
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT group_id, link FROM gm_links_map")
        return dict(c.fetchall())

# Translate Telethon exceptions to user-friendly reasons
def get_friendly_error(exception):
    err_str = str(exception).lower()
    if "write_forbidden" in err_str or "chatwriteforbidden" in err_str:
        return "Write forbidden (Account restricted, banned, or lacks permission to post)"
    elif "deactivated" in err_str or "authkeydeactivated" in err_str:
        return "Userbot account is deactivated or banned by Telegram"
    elif "flood" in err_str or "floodwait" in err_str:
        return "Flood wait limits hit (Temporarily restricted by Telegram due to spam rules)"
    elif "private" in err_str or "channelprivate" in err_str:
        return "Group is private or inaccessible (Not a member / invite expired)"
    elif "peer" in err_str or "invalid" in err_str:
        return "Group username or ID is invalid/dead"
    elif "paid" in err_str or "star" in err_str or "paywall" in err_str:
        return "Paywall enabled (Group requires Stars to send messages)"
    elif "banned" in err_str:
        return "Banned from the chat/group"
    elif "slow_mode" in err_str or "slowmode" in err_str:
        return "Slow mode is active in this chat"
    return f"Failed: {str(exception)[:60]}"

# Helper to join a group using Telethon
async def join_group_via_client(client, link):
    link = link.strip()
    if not link:
        return False
    try:
        if "+" in link or "joinchat/" in link:
            hash_val = link.split("+")[-1].strip() if "+" in link else link.split("joinchat/")[-1].strip()
            hash_val = hash_val.split("/")[0].split("?")[0]
            await client(ImportChatInviteRequest(hash_val))
            return True
        else:
            username = link
            if "t.me/" in link:
                username = link.split("t.me/")[-1].split("/")[0].split("?")[0]
            if not username.startswith("@") and not username.isdigit():
                username = "@" + username
            await client(JoinChannelRequest(username))
            return True
    except Exception as e:
        logger.error(f"Join failed for {link}: {e}")
        raise e

# Save the original get_dashboard_markup function safely to prevent double-wrapping
if get_dashboard_markup.__name__ != "new_get_dashboard_markup":
    original_get_dashboard_markup = get_dashboard_markup

    def new_get_dashboard_markup():
        markup = original_get_dashboard_markup()
        # Prevent duplicate buttons inside the markup
        already_has_button = False
        if hasattr(markup, 'keyboard') and markup.keyboard:
            for row in markup.keyboard:
                for btn in row:
                    if getattr(btn, 'callback_data', None) == "gm_tasks_main":
                        already_has_button = True
                        break
        if not already_has_button:
            markup.add(InlineKeyboardButton("📬 Group Mailer Tasks", callback_data="gm_tasks_main"))
        return markup

    # Monkeypatch the dashboard markup function
    main_module.get_dashboard_markup = new_get_dashboard_markup

# Dynamic fetching dialogs function
async def fetch_dialogs_async(client, ub_id):
    groups = []
    async for dialog in client.iter_dialogs(limit=200):
        if dialog.is_group or dialog.is_channel:
            groups.append({
                "id": dialog.id,
                "title": dialog.name,
                "username": dialog.entity.username if hasattr(dialog.entity, 'username') and dialog.entity.username else None
            })
    userbot_groups_cache[ub_id] = groups

# Render task control status description
def get_task_status_text(task):
    t_id, name, userbot_ids_raw, group_ids_raw, message_raw, interval, last_run_timestamp, update_group, media_sources_raw, media_interval, media_mix, media_enabled = task[:12]
    selected_ubs = json.loads(userbot_ids_raw or "[]")
    selected_groups = json.loads(group_ids_raw or "[]")
    msg_data = json.loads(message_raw or "{}")
    selected_sources = json.loads(media_sources_raw or "[]")
    
    ub_status = f"🔴 None"
    if selected_ubs:
        connected_count = 0
        for ub_id in selected_ubs:
            client = userbot_fleet_manager.get_client(int(ub_id))
            if client and client.is_connected():
                connected_count += 1
        ub_status = f"🟢 Configured ({connected_count}/{len(selected_ubs)} Connected)"
        
    msg_status = "🔴 None"
    if msg_data:
        msg_status = f"🟢 Configured ({msg_data.get('type').upper()})"
        
    rep_status = "🔴 Off (Manual Only)"
    if interval > 0:
        if interval < 60:
            rep_status = f"🟢 Every {interval} minutes"
        else:
            rep_status = f"🟢 Every {interval // 60} hour(s)"
            
    last_run_time = "Never"
    if last_run_timestamp > 0:
        last_run_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_run_timestamp))
        
    update_group_status = f"`{update_group}`" if update_group else "🔴 Muted (No log updates)"
    
    mode_lbl = "🖼 Fetch Media from Sources" if media_enabled else "💬 Send Static Message"
    mix_lbl = "🟢 Activated" if media_mix else "🔴 Deactivated"
    
    is_running = active_broadcasts.get(t_id, False)
    run_status = "🟢 Running" if is_running else "🔴 Idle"
    
    status_text = (
        f"📋 *Task Name:* `{name}` (ID: `{t_id}`)\n"
        f"⚡ *Campaign Status:* `{run_status}`\n"
        f"👤 *Selected Userbots:* {ub_status}\n"
        f"⚙️ *Mailer Mode:* `{mode_lbl}`\n"
    )
    
    if media_enabled:
        media_dedup = task[13] if len(task) > 13 else 0
        dedup_lbl = "🟢 Enabled" if media_dedup else "🔴 Disabled"
        status_text += (
            f"📥 *Source Chats:* `{len(selected_sources)}` configured\n"
            f"⏱ *Media Interval:* `{media_interval} seconds`\n"
            f"🔀 *Mix Mode:* {mix_lbl}\n"
            f"🛡️ *Duplicate Protection:* `{dedup_lbl}`\n"
        )
    else:
        status_text += f"💬 *Mailer Message:* {msg_status}\n"
        
    status_text += (
        f"👥 *Target Groups:* `{len(selected_groups)}` marked\n"
        f"📢 *Update Group:* {update_group_status}\n"
        f"⏰ *Repeat Interval:* `{rep_status}`\n"
        f"📅 *Last Run:* `{last_run_time}`"
    )
    return status_text

def get_task_control_markup(task):
    t_id = task[0]
    media_enabled = task[11]
    media_mix = task[10]
    
    markup = InlineKeyboardMarkup()
    mode_btn_text = "🖼 Mode: Fetch Media" if media_enabled else "💬 Mode: Static Msg"
    
    if media_enabled:
        # Row 1: Select Userbots, Target Groups
        markup.row(
            InlineKeyboardButton("👤 Select Userbots", callback_data=f"gm_taskubs_{t_id}"),
            InlineKeyboardButton("👥 Target Groups", callback_data=f"gm_taskgrps_{t_id}")
        )
        # Row 2: Source Chats, Media Interval
        markup.row(
            InlineKeyboardButton("📥 Source Chats", callback_data=f"gm_tasksrcgrps_{t_id}"),
            InlineKeyboardButton("⏱ Media Interval", callback_data=f"gm_taskmedint_{t_id}")
        )
        # Row 3: Mode Toggle, Mix Toggle, Dups Toggle
        mix_btn_text = "🔀 Mix: ON" if media_mix else "🔀 Mix: OFF"
        media_dedup = task[13] if len(task) > 13 else 0
        dup_btn_text = "🛡️ Dups: ON" if media_dedup else "🛡️ Dups: OFF"
        markup.row(
            InlineKeyboardButton(mode_btn_text, callback_data=f"gm_tasktglmode_{t_id}"),
            InlineKeyboardButton(mix_btn_text, callback_data=f"gm_tasktglmix_{t_id}"),
            InlineKeyboardButton(dup_btn_text, callback_data=f"gm_tasktgldup_{t_id}")
        )
    else:
        # Row 1: Select Userbots, Select Msg
        markup.row(
            InlineKeyboardButton("👤 Select Userbots", callback_data=f"gm_taskubs_{t_id}"),
            InlineKeyboardButton("💬 Select Msg", callback_data=f"gm_taskmsg_{t_id}")
        )
        # Row 2: Target Groups, Mode Toggle
        markup.row(
            InlineKeyboardButton("👥 Target Groups", callback_data=f"gm_taskgrps_{t_id}"),
            InlineKeyboardButton(mode_btn_text, callback_data=f"gm_tasktglmode_{t_id}")
        )
        
    # Row 4: Repeat Interval, Import Links
    markup.row(
        InlineKeyboardButton("⏰ Repeat Interval", callback_data=f"gm_taskrep_{t_id}"),
        InlineKeyboardButton("🔗 Import Join Links", callback_data=f"gm_tasklinks_{t_id}")
    )
    
    # Row 5: Start / Stop Operation
    is_running = active_broadcasts.get(t_id, False)
    if is_running:
        markup.row(
            InlineKeyboardButton("🛑 Stop Operation", callback_data=f"gm_taskstop_{t_id}")
        )
    else:
        markup.row(
            InlineKeyboardButton("🚀 Start Operation", callback_data=f"gm_taskstart_{t_id}")
        )
    
    # Row 6: Delete Task, Back to list
    markup.row(
        InlineKeyboardButton("🗑 Delete Task", callback_data=f"gm_taskdel_{t_id}"),
        InlineKeyboardButton("🔙 Back to Tasks", callback_data="gm_tasks_list")
    )
    return markup

def get_task_control_buttons_telethon(task):
    t_id = task[0]
    media_enabled = task[11]
    media_mix = task[10]
    
    from telethon import Button
    mode_btn_text = "🖼 Mode: Fetch Media" if media_enabled else "💬 Mode: Static Msg"
    
    is_running = active_broadcasts.get(t_id, False)
    op_button = Button.inline("🛑 Stop Operation", f"gm_taskstop_{t_id}") if is_running else Button.inline("🚀 Start Operation", f"gm_taskstart_{t_id}")
    
    if media_enabled:
        mix_btn_text = "🔀 Mix: ON" if media_mix else "🔀 Mix: OFF"
        media_dedup = task[13] if len(task) > 13 else 0
        dup_btn_text = "🛡️ Dups: ON" if media_dedup else "🛡️ Dups: OFF"
        return [
            [Button.inline("👤 Select Userbots", f"gm_taskubs_{t_id}"), Button.inline("👥 Target Groups", f"gm_taskgrps_{t_id}")],
            [Button.inline("📥 Source Chats", f"gm_tasksrcgrps_{t_id}"), Button.inline("⏱ Media Interval", f"gm_taskmedint_{t_id}")],
            [Button.inline(mode_btn_text, f"gm_tasktglmode_{t_id}"), Button.inline(mix_btn_text, f"gm_tasktglmix_{t_id}"), Button.inline(dup_btn_text, f"gm_tasktgldup_{t_id}")],
            [Button.inline("⏰ Repeat Interval", f"gm_taskrep_{t_id}"), Button.inline("🔗 Import Join Links", f"gm_tasklinks_{t_id}")],
            [op_button],
            [Button.inline("🗑 Delete Task", f"gm_taskdel_{t_id}"), Button.inline("🔙 Back to Tasks", "gm_tasks_list")]
        ]
    else:
        return [
            [Button.inline("👤 Select Userbots", f"gm_taskubs_{t_id}"), Button.inline("💬 Select Msg", f"gm_taskmsg_{t_id}")],
            [Button.inline("👥 Target Groups", f"gm_taskgrps_{t_id}"), Button.inline(mode_btn_text, f"gm_tasktglmode_{t_id}")],
            [Button.inline("⏰ Repeat Interval", f"gm_taskrep_{t_id}"), Button.inline("🔗 Import Join Links", f"gm_tasklinks_{t_id}")],
            [op_button],
            [Button.inline("🗑 Delete Task", f"gm_taskdel_{t_id}"), Button.inline("🔙 Back to Tasks", "gm_tasks_list")]
        ]

# Helper to render the interactive groups checklist page for a specific task
def show_task_groups_page(chat_id, message_id, task_id, ub_id, page=0):
    groups = userbot_groups_cache.get(ub_id, [])
    task = db_get_task(task_id)
    selected_ids = set(json.loads(task[3] or "[]"))
    update_group_id = task[7]
    
    if not groups:
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("🔄 Refresh List", callback_data=f"gm_tref_{task_id}_{page}"),
            InlineKeyboardButton("🔙 Back", callback_data=f"gm_task_view_{task_id}")
        )
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="👥 *Groups:* Userbot is not in any groups yet. Tap **Refresh List** to fetch groups dynamically.",
            reply_markup=markup,
            parse_mode="Markdown"
        )
        return

    page_size = 8
    total_pages = (len(groups) + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_groups = groups[start_idx:end_idx]
    
    markup = InlineKeyboardMarkup()
    for g in page_groups:
        is_selected = g["id"] in selected_ids
        checkbox = "✅" if is_selected else "⬜"
        title = g["title"][:25]
        markup.add(InlineKeyboardButton(f"{checkbox} {title}", callback_data=f"gm_ttg_{task_id}_{g['id']}_{page}"))
        
    # Navigation row
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"gm_tpage_{task_id}_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data="gm_noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"gm_tpage_{task_id}_{page+1}"))
    markup.row(*nav_row)
    
    # Bulk actions and Refresh row
    markup.row(
        InlineKeyboardButton("Select All", callback_data=f"gm_tselall_{task_id}_{page}"),
        InlineKeyboardButton("Clear All", callback_data=f"gm_tclrall_{task_id}_{page}"),
        InlineKeyboardButton("🔄 Refresh", callback_data=f"gm_tref_{task_id}_{page}")
    )

    # Log/Update group configuration row
    log_group_btn_text = f"📢 Group: {update_group_id}" if update_group_id else "📢 Set Update Group"
    markup.row(
        InlineKeyboardButton(log_group_btn_text, callback_data=f"gm_tsetgrp_{task_id}_{page}"),
        InlineKeyboardButton("❌ Remove Group", callback_data=f"gm_tdelgrp_{task_id}_{page}")
    )
    
    markup.add(InlineKeyboardButton("🔙 Back to Task Panel", callback_data=f"gm_task_view_{task_id}"))
    
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"👥 *SELECT TARGET GROUPS* (Selected: `{len(selected_ids)}`)\nToggle target checkboxes. Click **Refresh** to sync new groups:",
        reply_markup=markup,
        parse_mode="Markdown"
    )

def show_task_sources_page(chat_id, message_id, task_id, ub_id, page=0):
    groups = userbot_groups_cache.get(ub_id, [])
    task = db_get_task(task_id)
    selected_sources = set(json.loads(task[8] or "[]"))
    
    if not groups:
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("🔄 Refresh List", callback_data=f"gm_tsrcref_{task_id}_{page}"),
            InlineKeyboardButton("🔙 Back", callback_data=f"gm_task_view_{task_id}")
        )
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="📥 *Source Chats:* Userbot is not in any groups/channels yet. Tap **Refresh List** to fetch dialogs dynamically.",
            reply_markup=markup,
            parse_mode="Markdown"
        )
        return

    page_size = 8
    total_pages = (len(groups) + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_groups = groups[start_idx:end_idx]
    
    markup = InlineKeyboardMarkup()
    for g in page_groups:
        is_selected = g["id"] in selected_sources
        checkbox = "✅" if is_selected else "⬜"
        title = g["title"][:25]
        markup.add(InlineKeyboardButton(f"{checkbox} {title}", callback_data=f"gm_tsrc_{task_id}_{g['id']}_{page}"))
        
    # Navigation row
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"gm_tsrcpage_{task_id}_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data="gm_noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"gm_tsrcpage_{task_id}_{page+1}"))
    markup.row(*nav_row)
    
    # Bulk actions and Refresh row
    markup.row(
        InlineKeyboardButton("Select All", callback_data=f"gm_tsrcselall_{task_id}_{page}"),
        InlineKeyboardButton("Clear All", callback_data=f"gm_tsrcclrall_{task_id}_{page}"),
        InlineKeyboardButton("🔄 Refresh", callback_data=f"gm_tsrcref_{task_id}_{page}")
    )
    
    markup.add(InlineKeyboardButton("🔙 Back to Task Panel", callback_data=f"gm_task_view_{task_id}"))
    
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"📥 *SELECT SOURCE CHATS TO FETCH MEDIA* (Selected: `{len(selected_sources)}`)\nToggle source checkboxes. Click **Refresh** to sync:",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# Register callback query handler
@bot.callback_query_handler(func=lambda call: call.data.startswith("gm_task"))
def handle_tasks_callbacks(call):
    uid = call.from_user.id
    if not is_authorized_manager(uid):
        return

    data = call.data
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    if data == "gm_tasks_main":
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("📋 List Tasks", callback_data="gm_tasks_list"),
            InlineKeyboardButton("➕ Create Task", callback_data="gm_tasks_create")
        )
        markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="📬 *GROUP MAILER TASKS MANAGER*\n\nDefine separate message campaigns (tasks) with different userbots, target groups, and intervals.",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data == "gm_tasks_create":
        admin_states[uid] = "awaiting_gm_task_name"
        bot.send_message(chat_id, "📋 *CREATE CAMPAIGN TASK*\n\nPlease enter a name for your campaign task (e.g. `Promo Group A`):")
        bot.answer_callback_query(call.id)

    elif data == "gm_tasks_list":
        tasks = db_get_tasks()
        markup = InlineKeyboardMarkup()
        
        if not tasks:
            markup.add(InlineKeyboardButton("➕ Create Task", callback_data="gm_tasks_create"))
            markup.add(InlineKeyboardButton("🔙 Back", callback_data="gm_tasks_main"))
            bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="📋 *Group Mailer Tasks:* No tasks defined yet.", reply_markup=markup, parse_mode="Markdown")
            return

        for t_id, name, interval, last_run in tasks:
            rep_lbl = "Manual" if interval == 0 else (f"{interval}m" if interval < 60 else f"{interval//60}h")
            markup.add(InlineKeyboardButton(f"📋 {name} ({rep_lbl})", callback_data=f"gm_task_view_{t_id}"))

        markup.add(InlineKeyboardButton("🔙 Back", callback_data="gm_tasks_main"))
        bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="📋 *Group Mailer Tasks:* Select a task to configure/run:", reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("gm_task_view_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        if not task:
            bot.answer_callback_query(call.id, "❌ Task not found!")
            return

        # Save active chat ID for scheduler updates
        set_setting("gm_admin_chat_id", str(chat_id))

        markup = get_task_control_markup(task)
        status_desc = get_task_status_text(task)
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"📬 *TASK CONTROL PANEL*\n\n{status_desc}",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_taskubs_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        clients = userbot_fleet_manager.get_all_clients()
        connected_clients = [c for c in clients if c.is_connected()]
        
        if not connected_clients:
            bot.answer_callback_query(call.id, "❌ No active connected userbots found!")
            return

        markup = InlineKeyboardMarkup(row_width=1)
        for client in connected_clients:
            c_id = None
            for uid_key, client_val in userbot_fleet_manager.clients.items():
                if client_val is client:
                    c_id = uid_key
                    break
            if not c_id:
                c_id = client._me.id if hasattr(client, '_me') and client._me else None
            if not c_id:
                continue
                
            is_selected = c_id in selected_ubs
            checkbox = "✅" if is_selected else "⬜"
            
            me = getattr(client, '_me', None)
            first_name = me.first_name if me else "Userbot"
            username = f" @{me.username}" if (me and me.username) else f" (ID: {c_id})"
            
            markup.add(InlineKeyboardButton(f"{checkbox} {first_name}{username}", callback_data=f"gm_tasktglub_{t_id}_{c_id}"))
        
        markup.add(InlineKeyboardButton("🔙 Done / Back", callback_data=f"gm_task_view_{t_id}"))
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"👤 *Select Userbots for task `{task[1]}` (Multiple selection enabled):*",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_tasktglub_"):
        parts = data.split("_")
        t_id = int(parts[2])
        ub_id = int(parts[3])
        
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        if ub_id in selected_ubs:
            selected_ubs.remove(ub_id)
        else:
            selected_ubs.append(ub_id)
            
        db_update_task(t_id, "userbot_ids", json.dumps(selected_ubs))
        bot.answer_callback_query(call.id, "Preference updated!")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_taskubs_{t_id}", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_taskmsg_"):
        t_id = int(data.split("_")[-1])
        admin_states[uid] = f"awaiting_gm_taskmsg_{t_id}"
        bot.send_message(
            chat_id,
            "💬 *SET TASK MAILER MESSAGE*\n\n"
            "Please send or forward the message you want to broadcast for this campaign (can be text, photo, video, or document with captions)."
        )
        bot.answer_callback_query(call.id)

    elif data.startswith("gm_taskgrps_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        if not selected_ubs:
            bot.answer_callback_query(call.id, "⚠️ Please select at least one Userbot first!", show_alert=True)
            return

        primary_ub = str(selected_ubs[0])
        client = userbot_fleet_manager.get_client(int(primary_ub))
        if not client or not client.is_connected():
            bot.answer_callback_query(call.id, "❌ Primary userbot is offline or disconnected!", show_alert=True)
            return

        bot.answer_callback_query(call.id, "⏳ Loading groups...")
        if primary_ub in userbot_groups_cache:
            show_task_groups_page(chat_id, message_id, t_id, primary_ub, page=0)
        else:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⏳ *Fetching groups list from userbot. Please wait...*",
                parse_mode="Markdown"
            )
            def on_fetch_done(fut):
                show_task_groups_page(chat_id, message_id, t_id, primary_ub, page=0)
            
            future = asyncio.run_coroutine_threadsafe(fetch_dialogs_async(client, primary_ub), loop)
            future.add_done_callback(on_fetch_done)

    elif data.startswith("gm_tasksrcgrps_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        if not selected_ubs:
            bot.answer_callback_query(call.id, "⚠️ Please select at least one Userbot first!", show_alert=True)
            return

        primary_ub = str(selected_ubs[0])
        client = userbot_fleet_manager.get_client(int(primary_ub))
        if not client or not client.is_connected():
            bot.answer_callback_query(call.id, "❌ Primary userbot is offline or disconnected!", show_alert=True)
            return

        bot.answer_callback_query(call.id, "⏳ Loading source chats...")
        if primary_ub in userbot_groups_cache:
            show_task_sources_page(chat_id, message_id, t_id, primary_ub, page=0)
        else:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⏳ *Fetching source chats list from userbot. Please wait...*",
                parse_mode="Markdown"
            )
            def on_fetch_done(fut):
                show_task_sources_page(chat_id, message_id, t_id, primary_ub, page=0)
            
            future = asyncio.run_coroutine_threadsafe(fetch_dialogs_async(client, primary_ub), loop)
            future.add_done_callback(on_fetch_done)

    elif data.startswith("gm_tasktglmode_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        new_mode = 0 if task[11] else 1
        db_update_task(t_id, "media_enabled", new_mode)
        bot.answer_callback_query(call.id, f"Mode toggled to {'Media Fetcher' if new_mode else 'Static Message'}")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_task_view_{t_id}", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_tasktglmix_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        new_mix = 0 if task[10] else 1
        db_update_task(t_id, "media_mix", new_mix)
        bot.answer_callback_query(call.id, f"Mix Mode toggled to {'ON' if new_mix else 'OFF'}")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_task_view_{t_id}", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_tasklinks_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        if not selected_ubs:
            bot.answer_callback_query(call.id, "❌ Please select at least one Userbot first!", show_alert=True)
            return
            
        admin_states[uid] = f"awaiting_gm_tasklinks_{t_id}"
        bot.send_message(
            chat_id,
            "🔗 *IMPORT TASK GROUP JOIN LINKS*\n\n"
            "Please send your group links (invite links or usernames, one per line).\n"
            "Example:\n`t.me/+invitehash`\n`@my_group`"
        )
        bot.answer_callback_query(call.id)

    elif data.startswith("gm_taskrep_"):
        t_id = int(data.split("_")[-1])
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("❌ Off (Manual)", callback_data=f"gm_tasksetrep_{t_id}_0"),
            InlineKeyboardButton("30 Min", callback_data=f"gm_tasksetrep_{t_id}_30")
        )
        markup.row(
            InlineKeyboardButton("1 Hour", callback_data=f"gm_tasksetrep_{t_id}_60"),
            InlineKeyboardButton("2 Hours", callback_data=f"gm_tasksetrep_{t_id}_120")
        )
        markup.row(
            InlineKeyboardButton("6 Hours", callback_data=f"gm_tasksetrep_{t_id}_360"),
            InlineKeyboardButton("12 Hours", callback_data=f"gm_tasksetrep_{t_id}_720")
        )
        markup.row(
            InlineKeyboardButton("24 Hours", callback_data=f"gm_tasksetrep_{t_id}_1440")
        )
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"gm_task_view_{t_id}"))
        
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⏰ *SELECT REPEAT INTERVAL*\nConfigure how often this campaign task should automatically broadcast:",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_tasksetrep_"):
        parts = data.split("_")
        t_id = int(parts[2])
        minutes = int(parts[3])
        
        db_update_task(t_id, "repeat_interval", minutes)
        bot.answer_callback_query(call.id, "✅ Repeat interval updated!")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_task_view_{t_id}", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_taskmedint_"):
        t_id = int(data.split("_")[-1])
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("2 Sec", callback_data=f"gm_tasksetmedint_{t_id}_2"),
            InlineKeyboardButton("5 Sec", callback_data=f"gm_tasksetmedint_{t_id}_5")
        )
        markup.row(
            InlineKeyboardButton("10 Sec", callback_data=f"gm_tasksetmedint_{t_id}_10"),
            InlineKeyboardButton("15 Sec", callback_data=f"gm_tasksetmedint_{t_id}_15")
        )
        markup.row(
            InlineKeyboardButton("20 Sec", callback_data=f"gm_tasksetmedint_{t_id}_20"),
            InlineKeyboardButton("30 Sec", callback_data=f"gm_tasksetmedint_{t_id}_30")
        )
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"gm_task_view_{t_id}"))
        
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⏱ *SELECT MEDIA INTERVAL*\nConfigure the delay in seconds between each media message being sent:",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_tasksetmedint_"):
        parts = data.split("_")
        t_id = int(parts[2])
        seconds = int(parts[3])
        
        db_update_task(t_id, "media_interval", seconds)
        bot.answer_callback_query(call.id, f"✅ Media interval updated to {seconds}s!")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_task_view_{t_id}", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_taskdel_"):
        t_id = int(data.split("_")[-1])
        db_delete_task(t_id)
        bot.answer_callback_query(call.id, "🗑 Task deleted successfully!")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': "gm_tasks_list", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_taskstart_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        selected_groups = json.loads(task[3] or "[]")
        msg_data = json.loads(task[4] or "{}")
        media_sources = json.loads(task[8] or "[]")
        media_enabled = task[11]

        if not selected_ubs:
            bot.answer_callback_query(call.id, "❌ Please select at least one Userbot first!", show_alert=True)
            return
        if not selected_groups:
            bot.answer_callback_query(call.id, "❌ Please select target groups first!", show_alert=True)
            return
            
        if media_enabled:
            if not media_sources:
                bot.answer_callback_query(call.id, "❌ Please select media source chats first!", show_alert=True)
                return
        else:
            if not msg_data:
                bot.answer_callback_query(call.id, "❌ Please set the mailer message first!", show_alert=True)
                return

        bot.answer_callback_query(call.id, "🚀 Starting campaign operation...")
        asyncio.run_coroutine_threadsafe(
            run_task_broadcast(t_id),
            loop
        )

    elif data.startswith("gm_taskstop_"):
        t_id = int(data.split("_")[-1])
        active_broadcasts[t_id] = False
        bot.answer_callback_query(call.id, "🛑 Stopping campaign operation...")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_task_view_{t_id}", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_tasktgldup_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        new_dedup = 0 if (task[13] if len(task) > 13 else 0) else 1
        db_update_task(t_id, "media_dedup", new_dedup)
        bot.answer_callback_query(call.id, f"🛡️ Duplicate Protection: {'ON' if new_dedup else 'OFF'}")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_task_view_{t_id}", 'message': call.message, 'id': call.id})())

# Catch-all sub-handlers for page navigations and toggles on tasks
@bot.callback_query_handler(func=lambda call: call.data.startswith("gm_t"))
def handle_task_checklist_callbacks(call):
    uid = call.from_user.id
    if not is_authorized_manager(uid):
        return

    data = call.data
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    parts = data.split("_")

    # gm_ttg_{task_id}_{group_id}_{page}
    if data.startswith("gm_ttg_"):
        t_id = int(parts[2])
        g_id = int(parts[3])
        page = int(parts[4])
        
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        selected_ids = json.loads(task[3] or "[]")
        
        if g_id in selected_ids:
            selected_ids.remove(g_id)
        else:
            selected_ids.append(g_id)
            
        db_update_task(t_id, "group_ids", json.dumps(selected_ids))
        show_task_groups_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id)

    # gm_tsrc_{task_id}_{group_id}_{page}
    elif data.startswith("gm_tsrc_"):
        t_id = int(parts[2])
        g_id = int(parts[3])
        page = int(parts[4])
        
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        selected_sources = json.loads(task[8] or "[]")
        
        if g_id in selected_sources:
            selected_sources.remove(g_id)
        else:
            selected_sources.append(g_id)
            
        db_update_task(t_id, "media_sources", json.dumps(selected_sources))
        show_task_sources_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id)

    # gm_tsrcpage_{task_id}_{page}
    elif data.startswith("gm_tsrcpage_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        show_task_sources_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id)

    # gm_tpage_{task_id}_{page}
    elif data.startswith("gm_tpage_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        show_task_groups_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id)

    # gm_tselall_{task_id}_{page}
    elif data.startswith("gm_tselall_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        primary_ub = str(selected_ubs[0])
        groups = userbot_groups_cache.get(primary_ub, [])
        
        selected_ids = set(json.loads(task[3] or "[]"))
        for g in groups:
            selected_ids.add(g["id"])
            
        db_update_task(t_id, "group_ids", json.dumps(list(selected_ids)))
        show_task_groups_page(chat_id, message_id, t_id, primary_ub, page)
        bot.answer_callback_query(call.id, "✅ Selected all groups!")

    # gm_tsrcselall_{task_id}_{page}
    elif data.startswith("gm_tsrcselall_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        primary_ub = str(selected_ubs[0])
        groups = userbot_groups_cache.get(primary_ub, [])
        
        selected_sources = set(json.loads(task[8] or "[]"))
        for g in groups:
            selected_sources.add(g["id"])
            
        db_update_task(t_id, "media_sources", json.dumps(list(selected_sources)))
        show_task_sources_page(chat_id, message_id, t_id, primary_ub, page)
        bot.answer_callback_query(call.id, "✅ Selected all source chats!")

    # gm_tclrall_{task_id}_{page}
    elif data.startswith("gm_tclrall_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        db_update_task(t_id, "group_ids", "[]")
        show_task_groups_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id, "🗑 Cleared selections!")

    # gm_tsrcclrall_{task_id}_{page}
    elif data.startswith("gm_tsrcclrall_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        db_update_task(t_id, "media_sources", "[]")
        show_task_sources_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id, "🗑 Cleared source selections!")

    # gm_tref_{task_id}_{page}
    elif data.startswith("gm_tref_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        primary_ub = str(selected_ubs[0])
        
        client = userbot_fleet_manager.get_client(int(primary_ub))
        if not client or not client.is_connected():
            bot.answer_callback_query(call.id, "❌ Selected userbot is offline!", show_alert=True)
            return
            
        bot.answer_callback_query(call.id, "🔄 Syncing new groups...")
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔄 *Syncing new groups from Telegram... Please wait...*",
            parse_mode="Markdown"
        )
        
        if primary_ub in userbot_groups_cache:
            del userbot_groups_cache[primary_ub]
            
        def on_sync_done(fut):
            show_task_groups_page(chat_id, message_id, t_id, primary_ub, page)
            
        future = asyncio.run_coroutine_threadsafe(fetch_dialogs_async(client, primary_ub), loop)
        future.add_done_callback(on_sync_done)

    # gm_tsrcref_{task_id}_{page}
    elif data.startswith("gm_tsrcref_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        primary_ub = str(selected_ubs[0])
        
        client = userbot_fleet_manager.get_client(int(primary_ub))
        if not client or not client.is_connected():
            bot.answer_callback_query(call.id, "❌ Selected userbot is offline!", show_alert=True)
            return
            
        bot.answer_callback_query(call.id, "🔄 Syncing source chats...")
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔄 *Syncing source chats from Telegram... Please wait...*",
            parse_mode="Markdown"
        )
        
        if primary_ub in userbot_groups_cache:
            del userbot_groups_cache[primary_ub]
            
        def on_sync_done(fut):
            show_task_sources_page(chat_id, message_id, t_id, primary_ub, page)
            
        future = asyncio.run_coroutine_threadsafe(fetch_dialogs_async(client, primary_ub), loop)
        future.add_done_callback(on_sync_done)

    # gm_tsetgrp_{task_id}_{page}
    elif data.startswith("gm_tsetgrp_"):
        t_id = int(parts[2])
        admin_states[uid] = f"awaiting_gm_tasklog_{t_id}"
        bot.send_message(
            chat_id,
            "📢 *SET UPDATE/LOG GROUP FOR THIS TASK*\n\n"
            "Please send the Group Chat ID (e.g. `-1001234567890`) where updates and failure logs for this campaign should go."
        )
        bot.answer_callback_query(call.id)

    # gm_tdelgrp_{task_id}_{page}
    elif data.startswith("gm_tdelgrp_"):
        t_id = int(parts[2])
        page = int(parts[3])
        db_update_task(t_id, "update_group_id", "")
        bot.answer_callback_query(call.id, "❌ Log group removed!")
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        show_task_groups_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)


# Intercept message state inputs for Group Mailer Campaign Tasks
@bot.message_handler(func=lambda m: is_authorized_manager(m.from_user.id) and admin_states.get(m.from_user.id) and admin_states.get(m.from_user.id).startswith("awaiting_gm_"))
def handle_task_states(message):
    uid = message.from_user.id
    chat_id = message.chat.id
    state = admin_states.get(uid)
    text = message.text or ""

    if state == "awaiting_gm_task_name":
        cleaned_name = text.strip()
        if not cleaned_name:
            bot.reply_to(message, "❌ Name cannot be empty.")
            return
            
        task_id = db_create_task(cleaned_name)
        admin_states[uid] = None
        
        # Confirmation and direct redirect
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("⚙️ Configure Task", callback_data=f"gm_task_view_{task_id}"))
        bot.reply_to(
            message,
            f"✅ *Task `{cleaned_name}` Created!*",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif state.startswith("awaiting_gm_taskmsg_"):
        t_id = int(state.split("_")[-1])
        msg_type = "text"
        file_id = None
        caption = message.caption or ""
        local_path = None

        if message.photo:
            msg_type = "photo"
            file_id = message.photo[-1].file_id
        elif message.video:
            msg_type = "video"
            file_id = message.video.file_id
        elif message.document:
            msg_type = "document"
            file_id = message.document.file_id

        if file_id:
            try:
                msg_status = bot.reply_to(message, "⏳ Downloading media locally...")
                file_info = bot.get_file(file_id)
                downloaded_file = bot.download_file(file_info.file_path)
                
                ext = file_info.file_path.split(".")[-1]
                local_path = os.path.join(MEDIA_DIR, f"task_media_{t_id}_{uid}.{ext}")
                
                with open(local_path, "wb") as f:
                    f.write(downloaded_file)
                bot.delete_message(chat_id, msg_status.message_id)
            except Exception as e:
                bot.reply_to(message, f"❌ Media download failed: {e}")
                return

        msg_data = {
            "type": msg_type,
            "text": message.text or "",
            "caption": caption,
            "local_path": local_path
        }
        
        db_update_task(t_id, "message", json.dumps(msg_data))
        admin_states[uid] = None
        bot.reply_to(message, f"✅ *Mailer Message Saved for Task!* (Type: `{msg_type.upper()}`)", parse_mode="Markdown")

    elif state.startswith("awaiting_gm_tasklinks_"):
        t_id = int(state.split("_")[-1])
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        client = userbot_fleet_manager.get_client(int(selected_ubs[0]))
        if not client or not client.is_connected():
            bot.reply_to(message, "❌ Primary userbot is offline. Cannot check invite links.")
            return

        bot.reply_to(message, "⏳ Processing and resolving join links...")
        
        links_map = db_get_links_map()
        selected_groups = json.loads(task[3] or "[]")
        
        success_count = 0
        
        async def resolve_links_task():
            nonlocal success_count
            for line in lines:
                try:
                    group_id = None
                    if "+" in line or "joinchat/" in line:
                        hash_val = line.split("+")[-1].strip() if "+" in line else line.split("joinchat/")[-1].strip()
                        hash_val = hash_val.split("/")[0].split("?")[0]
                        invite_info = await client(CheckChatInviteRequest(hash_val))
                        if hasattr(invite_info, 'chat'):
                            group_id = invite_info.chat.id
                    else:
                        username = line
                        if "t.me/" in line:
                            username = line.split("t.me/")[-1].split("/")[0].split("?")[0]
                        if not username.startswith("@") and not username.isdigit():
                            username = "@" + username
                        entity = await client.get_entity(username)
                        group_id = entity.id

                    if group_id:
                        final_id = int(group_id)
                        db_save_link(final_id, line)
                        if final_id not in selected_groups:
                            selected_groups.append(final_id)
                        success_count += 1
                except Exception as e:
                    logger.error(f"Error resolving line {line}: {e}")

            db_update_task(t_id, "group_ids", json.dumps(selected_groups))
            admin_states[uid] = None
            bot.send_message(
                chat_id,
                f"✅ *Links Processed!*\nSuccessfully resolved and added `{success_count}` groups to your selections.",
                parse_mode="Markdown"
            )

        asyncio.run_coroutine_threadsafe(resolve_links_task(), loop)

    elif state.startswith("awaiting_gm_tasklog_"):
        t_id = int(state.split("_")[-1])
        cleaned_id = text.strip()
        
        if not (cleaned_id.startswith("-") and cleaned_id.replace("-", "").isdigit()):
            bot.reply_to(message, "❌ *Invalid Group ID!*\nGroup IDs must start with a minus (e.g. `-1001234567890`).")
            return
            
        db_update_task(t_id, "update_group_id", cleaned_id)
        admin_states[uid] = None
        bot.reply_to(
            message,
            f"✅ *Task Log Group Configured!*\nLogs will go to: `{cleaned_id}`.",
            parse_mode="Markdown"
        )


# Asynchronous campaign execution running failover and joining logic
async def run_task_broadcast(task_id, is_auto=False):
    if active_broadcasts.get(task_id):
        logger.warning(f"Campaign {task_id} is already running. Skipping execution.")
        return

    task = db_get_task(task_id)
    if not task:
        return
        
    t_id, name, userbot_ids_raw, group_ids_raw, message_raw, interval, _, update_group_id, media_sources_raw, media_interval, media_mix, media_enabled, last_source_ids_raw, media_dedup = task
    ub_ids = json.loads(userbot_ids_raw or "[]")
    selected_groups = json.loads(group_ids_raw or "[]")
    msg_data = json.loads(message_raw or "{}")
    
    success = 0
    failed = 0
    label = f"⏰ Scheduled Campaign: {name}" if is_auto else f"📬 Task Mailer: {name}"
    
    dest_chat = int(update_group_id) if (update_group_id and update_group_id.strip()) else None
    
    if not ub_ids or not selected_groups:
        return

    if not media_enabled:
        db_update_task(task_id, "last_run", time.time())

    active_broadcasts[task_id] = True
    try:
        # Use primary userbot to resolve entities/join links and manage log messages
        primary_ub = ub_ids[0]
        client = userbot_fleet_manager.get_client(int(primary_ub))
        if not client or not client.is_connected():
            if dest_chat:
                try:
                    bot.send_message(dest_chat, f"❌ *{label} Failed:* Primary userbot is offline.")
                except Exception:
                    pass
            return

        links_map = db_get_links_map()
        failed_details = []
        
        if media_enabled:
            # --- MEDIA MODE ---
            selected_sources = json.loads(media_sources_raw or "[]")
            if not selected_sources:
                if dest_chat:
                    try:
                        bot.send_message(dest_chat, f"❌ *{label} Failed:* No media sources configured.")
                    except Exception:
                        pass
                return
                
            progress_msg = None
            if dest_chat:
                try:
                    progress_msg = bot.send_message(dest_chat, f"⏳ *{label}:* Fetching media from sources...", parse_mode="Markdown")
                except Exception:
                    pass
                
            # Parse last processed message IDs cursor
            last_processed = json.loads(last_source_ids_raw or "{}")
            new_last_processed = dict(last_processed)
            
            # Fetch media messages from each source separately
            sources_media = {}
            for src_id in selected_sources:
                if not active_broadcasts.get(task_id):
                    break
                try:
                    group_media = []
                    last_id = last_processed.get(str(src_id))
                    max_id_seen = last_id
                    
                    # Fetch up to 1000 messages (large batch limit)
                    async for msg in client.iter_messages(src_id, limit=1000):
                        if not active_broadcasts.get(task_id):
                            break
                        if last_id and msg.id <= last_id:
                            break
                        if media_dedup:
                            with main_module.db_conn() as conn:
                                c_history = conn.cursor()
                                query_hist = "SELECT 1 FROM gm_sent_media_history WHERE task_id = ? AND source_chat_id = ? AND message_id = ?" if not main_module.USING_POSTGRES else "SELECT 1 FROM gm_sent_media_history WHERE task_id = %s AND source_chat_id = %s AND message_id = %s"
                                c_history.execute(query_hist, (t_id, str(src_id), msg.id))
                                if c_history.fetchone():
                                    continue
                        if msg.media:
                            group_media.append(msg)
                        if max_id_seen is None or msg.id > max_id_seen:
                            max_id_seen = msg.id
                            
                    if group_media:
                        # Reverse so oldest new messages are sent first
                        group_media.reverse()
                        
                        if media_mix:
                            random.shuffle(group_media)
                        sources_media[src_id] = group_media
                        
                    if max_id_seen is not None:
                        new_last_processed[str(src_id)] = max_id_seen
                except Exception as e:
                    logger.error(f"Error fetching media from {src_id}: {e}")
                    
            # Save new cursors to DB
            db_update_task(task_id, "last_source_ids", json.dumps(new_last_processed))
            
            media_items = []
            if sources_media:
                if media_mix:
                    # Balanced round-robin interleaving with randomized group order per round
                    active_sources = list(sources_media.keys())
                    while active_sources:
                        random.shuffle(active_sources)
                        next_sources = []
                        for src_id in active_sources:
                            if sources_media[src_id]:
                                media_items.append(sources_media[src_id].pop(0))
                                if sources_media[src_id]:
                                    next_sources.append(src_id)
                        active_sources = next_sources
                else:
                    # Sequential merge if mix is off (Group A, then Group B, etc.)
                    for src_id in selected_sources:
                        if src_id in sources_media:
                            media_items.extend(sources_media[src_id])
                
            if not media_items:
                if dest_chat:
                    try:
                        bot.send_message(dest_chat, f"❌ *{label} Completed:* No media messages found in source groups.", parse_mode="Markdown")
                    except Exception:
                        pass
                return
                
            total_media = len(media_items)
            total_targets = len(selected_groups)
            
            for idx, msg in enumerate(media_items):
                # Re-fetch task dynamically on every loop step
                live_task = db_get_task(task_id)
                if not live_task or not active_broadcasts.get(task_id):
                    break
                    
                for t_idx, group_id in enumerate(selected_groups):
                    if not active_broadcasts.get(task_id):
                        break
                    group_sent_successfully = False
                    group_errors = []
                    
                    # Failover through configured userbots
                    for ub_id in ub_ids:
                        if not active_broadcasts.get(task_id):
                            break
                        ub_client = userbot_fleet_manager.get_client(int(ub_id))
                        if not ub_client or not ub_client.is_connected():
                            group_errors.append((ub_id, "Userbot offline"))
                            continue
                            
                        try:
                            entity = group_id
                            try:
                                if isinstance(group_id, str) and group_id.startswith("@"):
                                    entity = await ub_client.get_entity(group_id)
                                elif isinstance(group_id, str) and group_id.isdigit():
                                    entity = int(group_id)
                            except Exception as ent_err:
                                join_link = links_map.get(str(group_id))
                                if join_link:
                                    try:
                                        await join_group_via_client(ub_client, join_link)
                                        await asyncio.sleep(random.randint(5, 10))
                                        entity = group_id
                                        if isinstance(group_id, str) and group_id.startswith("@"):
                                            entity = await ub_client.get_entity(group_id)
                                        elif isinstance(group_id, str) and group_id.isdigit():
                                            entity = int(group_id)
                                    except Exception as join_err:
                                        raise Exception(f"Auto-join failed: {join_err}")
                                else:
                                    raise ent_err
                                    
                            # Try direct send by file reference first
                            try:
                                await ub_client.send_file(entity, msg.media, caption=msg.message)
                            except Exception as send_err:
                                # Fallback: Download locally and upload/send
                                temp_file = await ub_client.download_media(msg)
                                if temp_file:
                                    await ub_client.send_file(entity, temp_file, caption=msg.message)
                                    try:
                                        os.remove(temp_file)
                                    except Exception:
                                        pass
                                else:
                                    raise send_err
                                    
                            group_sent_successfully = True
                            break
                        except Exception as e:
                            try:
                                # Retry auto-join check
                                join_link = links_map.get(str(group_id))
                                if join_link and "auto-join failed" not in str(e).lower():
                                    await join_group_via_client(ub_client, join_link)
                                    await asyncio.sleep(random.randint(5, 10))
                                    try:
                                        await ub_client.send_file(entity, msg.media, caption=msg.message)
                                    except Exception as inner_send_err:
                                        temp_file = await ub_client.download_media(msg)
                                        if temp_file:
                                            await ub_client.send_file(entity, temp_file, caption=msg.message)
                                            try: os.remove(temp_file)
                                            except Exception: pass
                                        else:
                                            raise inner_send_err
                                    group_sent_successfully = True
                                    break
                            except Exception as retry_err:
                                e = retry_err
                            group_errors.append((ub_id, e))
                            
                    if group_sent_successfully:
                        success += 1
                        if media_dedup:
                            try:
                                with main_module.db_conn() as conn:
                                    c_history = conn.cursor()
                                    query_insert = "INSERT OR IGNORE INTO gm_sent_media_history (task_id, source_chat_id, message_id) VALUES (?, ?, ?)" if not main_module.USING_POSTGRES else "INSERT INTO gm_sent_media_history (task_id, source_chat_id, message_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                                    c_history.execute(query_insert, (t_id, str(msg.chat_id), msg.id))
                                    conn.commit()
                            except Exception as hist_err:
                                logger.error(f"Error saving message {msg.id} to sent history: {hist_err}")
                    else:
                        failed += 1
                        report_lines = [f"❌ *Media {idx+1} to Target ID:* `{group_id}`"]
                        for ub_id, err in group_errors:
                            report_lines.append(f"  ⚠️ *UB {ub_id}:* {get_friendly_error(err)}")
                        failed_details.append("\n".join(report_lines))
                        
                    # Update progress
                    if progress_msg and dest_chat:
                        try:
                            pct = int((((idx * total_targets) + (t_idx + 1)) / (total_media * total_targets)) * 100)
                            bot.edit_message_text(
                                chat_id=dest_chat,
                                message_id=progress_msg.message_id,
                                text=(
                                    f"⏳ *{label} (Media Mode):* `{pct}%` Done\n"
                                    f"📷 Media: `{idx+1}/{total_media}` | Target: `{t_idx+1}/{total_targets}`\n"
                                    f"🟢 Success: `{success}` | 🔴 Failed: `{failed}`"
                                ),
                                parse_mode="Markdown"
                            )
                        except Exception:
                            pass
                            
                    # Small delay between target groups to prevent flooding
                    await asyncio.sleep(2)
                    
                # Wait configured interval between sending each media item
                await asyncio.sleep(media_interval)
                
            if dest_chat:
                try:
                    bot.send_message(
                        dest_chat,
                        f"✅ *{label} (Media Mode) Completed!*\n\n🟢 Success: `{success}`\n🔴 Failed: `{failed}`",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
                    
            # Send failure details
            if failed_details and dest_chat:
                try:
                    header = f"🚨 *{label} Failure Report (Media Mode):*\n\n"
                    current_message = header
                    for report in failed_details:
                        if len(current_message) + len(report) + 2 > 4000:
                            bot.send_message(dest_chat, current_message, parse_mode="Markdown")
                            current_message = ""
                        current_message += report + "\n\n"
                    if current_message.strip():
                        bot.send_message(dest_chat, current_message, parse_mode="Markdown")
                except Exception as e:
                    logger.error(f"Error sending failure report: {e}")
                    
        else:
            # --- ORIGINAL STATIC MESSAGE MODE ---
            progress_msg = None
            if dest_chat:
                try:
                    progress_msg = bot.send_message(dest_chat, f"⏳ *{label} progress:* `0%`", parse_mode="Markdown")
                except Exception as err:
                    logger.error(f"Failed to send task progress updates to {dest_chat}: {err}")

            sent_group_ids = set()
            
            # Pre-load group titles from local cache
            group_titles = {}
            for ub_id in ub_ids:
                for g in userbot_groups_cache.get(str(ub_id), []):
                    group_titles[g["id"]] = g["title"]

            while True:
                # Re-fetch task to load live group ids dynamically on every loop step
                live_task = db_get_task(task_id)
                if not live_task or not active_broadcasts.get(task_id):
                    break
                    
                live_group_ids = json.loads(live_task[3] or "[]")
                
                remaining_groups = [g for g in live_group_ids if g not in sent_group_ids]
                if not remaining_groups:
                    break
                    
                group_id = remaining_groups[0]
                sent_group_ids.add(group_id)
                
                group_sent_successfully = False
                group_errors = []

                # Iterate over all selected userbots
                for ub_id in ub_ids:
                    if not active_broadcasts.get(task_id):
                        break
                    client = userbot_fleet_manager.get_client(int(ub_id))
                    if not client or not client.is_connected():
                        group_errors.append((ub_id, "Userbot offline"))
                        continue

                    try:
                        entity = group_id
                        try:
                            if isinstance(group_id, str) and group_id.startswith("@"):
                                entity = await client.get_entity(group_id)
                            elif isinstance(group_id, str) and group_id.isdigit():
                                entity = int(group_id)
                        except Exception as ent_err:
                            join_link = links_map.get(str(group_id))
                            if join_link:
                                try:
                                    await join_group_via_client(client, join_link)
                                    join_wait = random.randint(5, 10)
                                    await asyncio.sleep(join_wait)
                                    entity = group_id
                                    if isinstance(group_id, str) and group_id.startswith("@"):
                                        entity = await client.get_entity(group_id)
                                    elif isinstance(group_id, str) and group_id.isdigit():
                                        entity = int(group_id)
                                except Exception as join_err:
                                    raise Exception(f"Auto-join failed: {join_err}")
                            else:
                                raise ent_err

                        # Send
                        msg_type = msg_data.get("type")
                        if msg_type == "text":
                            await client.send_message(entity, msg_data["text"])
                        elif msg_type in ["photo", "video", "document"]:
                            await client.send_file(entity, msg_data["local_path"], caption=msg_data.get("caption", ""))
                        
                        group_sent_successfully = True
                        break
                    except Exception as e:
                        try:
                            join_link = links_map.get(str(group_id))
                            if join_link and "auto-join failed" not in str(e).lower():
                                await join_group_via_client(client, join_link)
                                join_wait = random.randint(5, 10)
                                await asyncio.sleep(join_wait)
                                
                                msg_type = msg_data.get("type")
                                if msg_type == "text":
                                    await client.send_message(entity, msg_data["text"])
                                elif msg_type in ["photo", "video", "document"]:
                                    await client.send_file(entity, msg_data["local_path"], caption=msg_data.get("caption", ""))
                                
                                group_sent_successfully = True
                                break
                        except Exception as retry_err:
                            e = retry_err
                        
                        group_errors.append((ub_id, e))
                        logger.warning(f"Userbot {ub_id} failed to send to {group_id} under Task {task_id}: {e}")
                
                if group_sent_successfully:
                    success += 1
                else:
                    failed += 1
                    g_title = group_titles.get(group_id, f"ID: {group_id}")
                    report_lines = [f"❌ *Group:* `{g_title}`"]
                    for ub_id, err in group_errors:
                        report_lines.append(f"  ⚠️ *UB {ub_id}:* {get_friendly_error(err)}")
                    failed_details.append("\n".join(report_lines))

                # Live progress update
                total_groups = len(live_group_ids)
                processed_count = len(sent_group_ids)
                
                if progress_msg and dest_chat and (processed_count % 3 == 0 or processed_count == total_groups):
                    pct = int((processed_count / max(1, total_groups)) * 100)
                    try:
                        bot.edit_message_text(
                            chat_id=dest_chat,
                            message_id=progress_msg.message_id,
                            text=f"⏳ *{label} progress:* `{pct}%` (Success: `{success}`, Failed: `{failed}` | Total: `{total_groups}`)",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass

                # Random delay between 5 to 10 seconds
                await asyncio.sleep(random.randint(5, 10))

            if dest_chat:
                try:
                    bot.send_message(
                        dest_chat,
                        f"✅ *{label} Completed!*\n\n🟢 Success: `{success}`\n🔴 Failed: `{failed}`",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass

            # Send Detailed Failure Report
            if failed_details and dest_chat:
                try:
                    header = f"🚨 *{label} Failure Report:*\nThe message could not be sent to these groups on all configured accounts:\n\n"
                    current_message = header
                    
                    for report in failed_details:
                        if len(current_message) + len(report) + 2 > 4000:
                            bot.send_message(dest_chat, current_message, parse_mode="Markdown")
                            current_message = ""
                        current_message += report + "\n\n"
                        
                    if current_message.strip():
                        bot.send_message(dest_chat, current_message, parse_mode="Markdown")
                except Exception as e:
                    logger.error(f"Error sending failure report: {e}")
    finally:
        if media_enabled:
            db_update_task(task_id, "last_run", time.time())
        active_broadcasts.pop(task_id, None)


# Background Scheduled Supervisor Loop for Campaign Tasks
async def scheduler_loop():
    logger.info("⏰ Group Mailer Tasks scheduler supervisor loop running...")
    while True:
        try:
            # Query all tasks with repeat schedules
            with main_module.db_conn() as conn:
                c = conn.cursor()
                c.execute("SELECT id, name, repeat_interval, last_run, userbot_ids, group_ids, message, media_sources, media_enabled FROM gm_tasks WHERE repeat_interval > 0")
                tasks = c.fetchall()
                
            for t_id, name, interval, last_run, userbot_ids_raw, group_ids_raw, message_raw, media_sources_raw, media_enabled in tasks:
                now = time.time()
                if now - last_run >= (interval * 60):
                    ub_ids = json.loads(userbot_ids_raw or "[]")
                    selected_groups = json.loads(group_ids_raw or "[]")
                    msg_data = json.loads(message_raw or "{}")
                    selected_sources = json.loads(media_sources_raw or "[]")
                    
                    has_active_client = False
                    if ub_ids:
                        for ub_id in ub_ids:
                            client = userbot_fleet_manager.get_client(int(ub_id))
                            if client and client.is_connected():
                                has_active_client = True
                                break
                                
                    if has_active_client and selected_groups:
                        if media_enabled:
                            if selected_sources:
                                # Start campaign task asynchronously
                                await run_task_broadcast(t_id, is_auto=True)
                        else:
                            if msg_data:
                                # Start campaign task asynchronously
                                await run_task_broadcast(t_id, is_auto=True)
        except Exception as e:
            logger.error(f"Error in Tasks scheduler loop: {e}")
            
        await asyncio.sleep(30)  # Check every 30 seconds

# Start the background schedule task safely
asyncio.run_coroutine_threadsafe(scheduler_loop(), loop)

# --- Keyword Monitoring Commands System Integration ---
from telethon import events

def is_postgres():
    return getattr(main_module, 'USING_POSTGRES', False) or bool(getattr(main_module, 'DATABASE_URL', ''))

def get_placeholder():
    return "%s" if is_postgres() else "?"

def add_keyword(word):
    clean_word = word.strip().lower()
    if not clean_word: return
    with main_module.db_conn() as conn:
        c = conn.cursor()
        if is_postgres():
            c.execute("INSERT INTO keyword_checks (keyword) VALUES (%s) ON CONFLICT DO NOTHING", (clean_word,))
        else:
            c.execute("INSERT OR IGNORE INTO keyword_checks (keyword) VALUES (?)", (clean_word,))
        conn.commit()
    
    # Sync cache in keyword_monitor plugin if loaded
    km = sys.modules.get('plugins.keyword_monitor')
    if km and hasattr(km, 'reload_keywords_cache'):
        try: km.reload_keywords_cache()
        except Exception: pass

def remove_keyword(word):
    clean_word = word.strip().lower()
    if not clean_word: return
    with main_module.db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM keyword_checks WHERE keyword = {p}", (clean_word,))
        conn.commit()
    
    km = sys.modules.get('plugins.keyword_monitor')
    if km and hasattr(km, 'reload_keywords_cache'):
        try: km.reload_keywords_cache()
        except Exception: pass

def get_keywords():
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT keyword FROM keyword_checks")
        return [row[0] for row in c.fetchall()]

def add_keyword_monitored_group(chat_id, title):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        if is_postgres():
            c.execute("INSERT INTO keyword_monitored_groups (chat_id, title) VALUES (%s, %s) ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title", (chat_id, title))
        else:
            c.execute("INSERT OR REPLACE INTO keyword_monitored_groups (chat_id, title) VALUES (?, ?)", (chat_id, title))
        conn.commit()

def remove_keyword_monitored_group(chat_id):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM keyword_monitored_groups WHERE chat_id = {p}", (chat_id,))
        conn.commit()

def get_keyword_monitored_groups():
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT chat_id, title FROM keyword_monitored_groups ORDER BY added_at DESC")
        return c.fetchall()

def get_alternate_ids(chat_id):
    ids = {chat_id}
    if str(chat_id).startswith("-100"):
        try: ids.add(int(str(chat_id)[4:]))
        except ValueError: pass
    elif str(chat_id).startswith("-"):
        try: ids.add(int(str(chat_id)[1:]))
        except ValueError: pass
    else:
        try:
            val = int(chat_id)
            ids.add(-val)
            ids.add(int(f"-100{val}"))
        except ValueError: pass
    return ids

# --- Telebot Main Bot Commands ---
@bot.message_handler(commands=['addkey'])
def mailer_cmd_add_key(message):
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
def mailer_cmd_del_key(message):
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
def mailer_cmd_change_key(message):
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
def mailer_cmd_list_keys(message):
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
def mailer_cmd_monitor_group(message):
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
                    resolve_target_id = getattr(main_module, 'resolve_target_id', None)
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
def mailer_cmd_unmonitor_group(message):
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
                        resolve_target_id = getattr(main_module, 'resolve_target_id', None)
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
def mailer_cmd_list_groups(message):
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

# --- Telethon Userbot DM Commands ---
def setup_group_mailer_handlers(client):
    if getattr(client, '_group_mailer_handlers_registered', False):
        return
    client._group_mailer_handlers_registered = True

    # Helper to render groups checklist in Telethon DMs
    async def show_task_groups_page_telethon_helper(client_inst, event, task_id, page=0):
        task = db_get_task(task_id)
        selected_ubs = json.loads(task[2] or "[]")
        if not selected_ubs:
            await event.reply("❌ **Please select a userbot first!**")
            return
        primary_ub = str(selected_ubs[0])
        groups = userbot_groups_cache.get(primary_ub, [])
        selected_ids = set(json.loads(task[3] or "[]"))
        update_group_id = task[7]
        
        from telethon import Button
        if not groups:
            buttons = [
                [Button.inline("🔄 Refresh List", f"gm_tref_{task_id}_{page}")],
                [Button.inline("🔙 Back to Task Panel", f"gm_task_view_{task_id}")]
            ]
            await event.edit("👥 *Groups:* Userbot is not in any groups yet. Tap **Refresh List** to fetch groups dynamically.", buttons=buttons)
            return

        page_size = 8
        total_pages = (len(groups) + page_size - 1) // page_size
        page = max(0, min(page, total_pages - 1))
        
        start_idx = page * page_size
        end_idx = start_idx + page_size
        page_groups = groups[start_idx:end_idx]
        
        buttons = []
        for g in page_groups:
            is_selected = g["id"] in selected_ids
            checkbox = "✅" if is_selected else "⬜"
            title = g["title"][:25]
            buttons.append([Button.inline(f"{checkbox} {title}", f"gm_ttg_{task_id}_{g['id']}_{page}")])
            
        # Navigation row
        nav_row = []
        if page > 0:
            nav_row.append(Button.inline("◀️ Prev", f"gm_tpage_{task_id}_{page-1}"))
        nav_row.append(Button.inline(f"Page {page+1}/{total_pages}", "gm_noop"))
        if page < total_pages - 1:
            nav_row.append(Button.inline("Next ▶️", f"gm_tpage_{task_id}_{page+1}"))
        buttons.append(nav_row)
        
        # Bulk actions and Refresh row
        buttons.append([
            Button.inline("Select All", f"gm_tselall_{task_id}_{page}"),
            Button.inline("Clear All", f"gm_tclrall_{task_id}_{page}"),
            Button.inline("🔄 Refresh", f"gm_tref_{task_id}_{page}")
        ])

        # Log group row
        log_group_btn_text = f"📢 Group: {update_group_id}" if update_group_id else "📢 Set Update Group"
        buttons.append([
            Button.inline(log_group_btn_text, f"gm_tsetgrp_{task_id}_{page}"),
            Button.inline("❌ Remove Group", f"gm_tdelgrp_{task_id}_{page}")
        ])
        
        buttons.append([Button.inline("🔙 Back to Task Panel", f"gm_task_view_{task_id}")])
        
        await event.edit(
            f"👥 **SELECT TARGET GROUPS** (Selected: `{len(selected_ids)}`)\nToggle target checkboxes. Click **Refresh** to sync new groups:",
            buttons=buttons
        )

    async def show_task_sources_page_telethon_helper(client_inst, event, task_id, page=0):
        task = db_get_task(task_id)
        selected_ubs = json.loads(task[2] or "[]")
        if not selected_ubs:
            await event.reply("❌ **Please select a userbot first!**")
            return
        primary_ub = str(selected_ubs[0])
        groups = userbot_groups_cache.get(primary_ub, [])
        selected_sources = set(json.loads(task[8] or "[]"))
        
        from telethon import Button
        if not groups:
            buttons = [
                [Button.inline("🔄 Refresh List", f"gm_tsrcref_{task_id}_{page}")],
                [Button.inline("🔙 Back to Task Panel", f"gm_task_view_{task_id}")]
            ]
            await event.edit("📥 *Source Chats:* Userbot is not in any groups/channels yet. Tap **Refresh List** to fetch dialogs dynamically.", buttons=buttons)
            return

        page_size = 8
        total_pages = (len(groups) + page_size - 1) // page_size
        page = max(0, min(page, total_pages - 1))
        
        start_idx = page * page_size
        end_idx = start_idx + page_size
        page_groups = groups[start_idx:end_idx]
        
        buttons = []
        for g in page_groups:
            is_selected = g["id"] in selected_sources
            checkbox = "✅" if is_selected else "⬜"
            title = g["title"][:25]
            buttons.append([Button.inline(f"{checkbox} {title}", f"gm_tsrc_{task_id}_{g['id']}_{page}")])
            
        # Navigation row
        nav_row = []
        if page > 0:
            nav_row.append(Button.inline("◀️ Prev", f"gm_tsrcpage_{task_id}_{page-1}"))
        nav_row.append(Button.inline(f"Page {page+1}/{total_pages}", "gm_noop"))
        if page < total_pages - 1:
            nav_row.append(Button.inline("Next ▶️", f"gm_tsrcpage_{task_id}_{page+1}"))
        buttons.append(nav_row)
        
        # Bulk actions and Refresh row
        buttons.append([
            Button.inline("Select All", f"gm_tsrcselall_{task_id}_{page}"),
            Button.inline("Clear All", f"gm_tsrcclrall_{task_id}_{page}"),
            Button.inline("🔄 Refresh", f"gm_tsrcref_{task_id}_{page}")
        ])
        
        buttons.append([Button.inline("🔙 Back to Task Panel", f"gm_task_view_{task_id}")])
        
        await event.edit(
            f"📥 **SELECT SOURCE CHATS TO FETCH MEDIA** (Selected: `{len(selected_sources)}`)\nToggle source checkboxes. Click **Refresh** to sync:",
            buttons=buttons
        )

    @client.on(events.NewMessage(incoming=True))
    async def group_mailer_private_cmd_handler(event):
        m = event.message
        if not m or not m.text:
            return

        if not event.is_private:
            return

        if not hasattr(client, '_me') or not client._me:
            try:
                client._me = await client.get_me()
            except Exception as e:
                logger.error(f"Failed to get_me() in group mailer plugin: {e}")

        me = getattr(client, '_me', None)

        fleet_user_ids = {c._me.id for c in userbot_fleet_manager.get_all_clients() if getattr(c, '_me', None)}
        if me:
            fleet_user_ids.add(me.id)

        if m.sender_id in fleet_user_ids:
            return

        is_primary_admin = (m.sender_id == ADMIN_ID) or (me and m.sender_id == me.id)
        is_manager = is_primary_admin or is_authorized_manager(m.sender_id)
        if not is_manager:
            return

        # Check DM Input States
        state = admin_states.get(m.sender_id)
        if state:
            if state == "awaiting_gm_task_name_telethon":
                task_name = m.text.strip()
                if task_name:
                    t_id = db_create_task(task_name)
                    admin_states.pop(m.sender_id, None)
                    task = db_get_task(t_id)
                    status_desc = get_task_status_text(task)
                    buttons = get_task_control_buttons_telethon(task)
                    await event.reply(f"✅ **Task Created:** `{task_name}`\n\n📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)
                else:
                    await event.reply("❌ Invalid input. Task name cannot be empty.")
                return

            elif state.startswith("awaiting_gm_taskmsg_telethon_"):
                t_id = int(state.split("_")[-1])
                msg_type = "text"
                local_path = ""
                caption = m.message or ""
                
                if m.photo:
                    msg_type = "photo"
                elif m.video:
                    msg_type = "video"
                elif m.document:
                    msg_type = "document"
                    
                if m.media:
                    try:
                        reply_msg = await event.reply("⏳ Downloading media locally...")
                        path = await client.download_media(m)
                        if path:
                            ext = path.split(".")[-1]
                            local_path = os.path.join(MEDIA_DIR, f"task_media_{t_id}_{m.sender_id}.{ext}")
                            if os.path.exists(local_path):
                                os.remove(local_path)
                            os.rename(path, local_path)
                        await client.delete_messages(event.chat_id, [reply_msg.id])
                    except Exception as e:
                        await event.reply(f"❌ Media download failed: {e}")
                        return
                        
                msg_data = {
                    "type": msg_type,
                    "text": m.text or "",
                    "caption": caption,
                    "local_path": local_path
                }
                
                db_update_task(t_id, "message", json.dumps(msg_data))
                admin_states.pop(m.sender_id, None)
                
                task = db_get_task(t_id)
                status_desc = get_task_status_text(task)
                buttons = get_task_control_buttons_telethon(task)
                await event.reply(f"✅ **Mailer Message Saved for Task!** (Type: `{msg_type.upper()}`)\n\n📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)
                return

            elif state.startswith("awaiting_gm_tasklog_telethon_"):
                parts = state.split("_")
                t_id = int(parts[4])
                page = int(parts[5])
                
                log_chat = m.text.strip()
                db_update_task(t_id, "update_group_id", log_chat)
                admin_states.pop(m.sender_id, None)
                await event.reply(f"✅ Log/update group updated to `{log_chat}`")
                await show_task_groups_page_telethon_helper(client, event, t_id, page)
                return

            elif state.startswith("awaiting_gm_tasklinks_telethon_"):
                t_id = int(state.split("_")[-1])
                lines = [line.strip() for line in m.text.split("\n") if line.strip()]
                
                task = db_get_task(t_id)
                selected_ubs = json.loads(task[2] or "[]")
                
                if not selected_ubs:
                    await event.reply("❌ Please select a userbot first!")
                    admin_states.pop(m.sender_id, None)
                    return
                    
                target_client = userbot_fleet_manager.get_client(int(selected_ubs[0]))
                if not target_client or not target_client.is_connected():
                    await event.reply("❌ Primary userbot is offline. Cannot check invite links.")
                    admin_states.pop(m.sender_id, None)
                    return
                    
                progress_msg = await event.reply("⏳ Processing and resolving join links...")
                admin_states.pop(m.sender_id, None)
                
                async def run_import():
                    success_count = 0
                    fail_count = 0
                    group_ids = set(json.loads(task[3] or "[]"))
                    
                    for link in lines:
                        try:
                            username = link
                            if "t.me/" in link:
                                username = link.split("t.me/")[-1].split("/")[0].split("?")[0]
                            if not username.startswith("@") and not username.isdigit() and "+" not in link and "joinchat/" not in link:
                                username = "@" + username
                                
                            if "+" in link or "joinchat/" in link:
                                hash_val = link.split("+")[-1].strip() if "+" in link else link.split("joinchat/")[-1].strip()
                                hash_val = hash_val.split("/")[0].split("?")[0]
                                result = await target_client(ImportChatInviteRequest(hash_val))
                                if hasattr(result, "chats") and result.chats:
                                    chat_entity = result.chats[0]
                                    from telethon.utils import get_peer_id
                                    cid = get_peer_id(chat_entity)
                                    group_ids.add(cid)
                                    db_save_link(cid, link)
                                    success_count += 1
                            else:
                                chat_entity = await target_client.get_entity(username)
                                from telethon.utils import get_peer_id
                                cid = get_peer_id(chat_entity)
                                await target_client(JoinChannelRequest(chat_entity))
                                group_ids.add(cid)
                                db_save_link(cid, link)
                                success_count += 1
                        except Exception as ex:
                            logger.error(f"Invite check failed: {ex}")
                            fail_count += 1
                            
                    db_update_task(t_id, "group_ids", json.dumps(list(group_ids)))
                    try:
                        await fetch_dialogs_async(target_client, str(selected_ubs[0]))
                    except Exception:
                        pass
                        
                    await progress_msg.edit(f"✅ **Import Complete!**\n\n🟢 Successfully joined & marked: `{success_count}`\n🔴 Failed: `{fail_count}`")
                    
                    task_updated = db_get_task(t_id)
                    status_desc = get_task_status_text(task_updated)
                    buttons = get_task_control_buttons_telethon(task_updated)
                    await event.reply(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)
                    
                asyncio.create_task(run_import())
                return

        text = m.text.strip()
        if not text.startswith('.'):
            return

        parts = text.split(None, 1)
        cmd = parts[0].lower()

        # Built-in Commands handling
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
                    resolve_target_id = getattr(main_module, 'resolve_target_id', None)
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
                    resolve_target_id = getattr(main_module, 'resolve_target_id', None)
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

        elif cmd == '.mailer':
            from telethon import Button
            buttons = [
                [Button.inline("📋 List Tasks", "gm_tasks_list"), Button.inline("➕ Create Task", "gm_tasks_create")]
            ]
            await event.reply("📬 **GROUP MAILER TASKS MANAGER**\n\nDefine separate message campaigns (tasks) with different userbots, target groups, and intervals.", buttons=buttons)
            return

        elif cmd in ['.creat', '.create']:
            if len(parts) < 2:
                await event.reply("❌ **Usage:** `.create <task_name> <message>`\nExample: `.create MyCampaign Hello world!` or `.create \"My Campaign\" Hello world!`")
                return
            arg_text = parts[1].strip()
            
            task_name = ""
            msg_text = ""
            if arg_text.startswith('"'):
                end_quote_idx = arg_text.find('"', 1)
                if end_quote_idx != -1:
                    task_name = arg_text[1:end_quote_idx].strip()
                    msg_text = arg_text[end_quote_idx+1:].strip()
            
            if not task_name:
                subparts = arg_text.split(None, 1)
                task_name = subparts[0].strip()
                msg_text = subparts[1].strip() if len(subparts) > 1 else ""
                
            if not task_name or not msg_text:
                await event.reply("❌ **Usage:** `.create <task_name> <message>`\nExample: `.create MyCampaign Hello world!` or `.create \"My Campaign\" Hello world!`")
                return
                
            t_id = db_create_task(task_name)
            msg_data = {
                "type": "text",
                "text": msg_text,
                "caption": msg_text,
                "local_path": ""
            }
            db_update_task(t_id, "message", json.dumps(msg_data))
            
            task = db_get_task(t_id)
            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.reply(f"✅ **Task Created Successfully!**\n\n📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)
            return

        # Check Dynamic Taskname matching
        clean_cmd = cmd[1:].strip().lower().replace(" ", "")
        tasks = db_get_tasks()
        matched_task_id = None
        for t_id, name, _, _ in tasks:
            normalized_name = name.strip().lower().replace(" ", "")
            if clean_cmd == normalized_name:
                matched_task_id = t_id
                break
                
        if matched_task_id:
            task = db_get_task(matched_task_id)
            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.reply(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)
            return

    # --- Telethon Callback Queries Handler ---
    @client.on(events.CallbackQuery(data=re.compile(b'^gm_')))
    async def gm_telethon_callback_handler(event):
        sender_id = event.sender_id
        
        # Check permissions
        is_primary_admin = (sender_id == ADMIN_ID) or (me and sender_id == me.id)
        is_manager = is_primary_admin or is_authorized_manager(sender_id)
        if not is_manager:
            await event.answer("❌ Unauthorized", alert=True)
            return

        data = event.data.decode('utf-8')
        parts = data.split("_")
        from telethon import Button

        if data == "gm_tasks_main":
            buttons = [
                [Button.inline("📋 List Tasks", "gm_tasks_list"), Button.inline("➕ Create Task", "gm_tasks_create")]
            ]
            await event.edit("📬 **GROUP MAILER TASKS MANAGER**\n\nDefine separate message campaigns (tasks) with different userbots, target groups, and intervals.", buttons=buttons)
            await event.answer()

        elif data == "gm_tasks_create":
            admin_states[sender_id] = "awaiting_gm_task_name_telethon"
            await event.reply("📋 **CREATE CAMPAIGN TASK**\n\nPlease enter a name for your campaign task (e.g. `Promo Group A`):")
            await event.answer()

        elif data == "gm_tasks_list":
            tasks = db_get_tasks()
            if not tasks:
                buttons = [
                    [Button.inline("➕ Create Task", "gm_tasks_create")],
                    [Button.inline("🔙 Back", "gm_tasks_main")]
                ]
                await event.edit("📋 **Group Mailer Tasks:** No tasks defined yet.", buttons=buttons)
                await event.answer()
                return

            buttons = []
            for t_id, name, interval, last_run in tasks:
                rep_lbl = "Manual" if interval == 0 else (f"{interval}m" if interval < 60 else f"{interval//60}h")
                buttons.append([Button.inline(f"📋 {name} ({rep_lbl})", f"gm_task_view_{t_id}")])
            buttons.append([Button.inline("🔙 Back", "gm_tasks_main")])
            await event.edit("📋 **Group Mailer Tasks:** Select a task to configure/run:", buttons=buttons)
            await event.answer()

        elif data.startswith("gm_task_view_"):
            t_id = int(parts[-1])
            task = db_get_task(t_id)
            if not task:
                await event.answer("❌ Task not found!", alert=True)
                return

            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.edit(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)
            await event.answer()

        elif data.startswith("gm_taskubs_"):
            t_id = int(parts[-1])
            task = db_get_task(t_id)
            selected_ubs = json.loads(task[2] or "[]")
            
            clients = userbot_fleet_manager.get_all_clients()
            connected_clients = [c for c in clients if c.is_connected()]
            
            if not connected_clients:
                await event.answer("❌ No active connected userbots found!", alert=True)
                return

            buttons = []
            for cl in connected_clients:
                c_id = None
                for uid_key, client_val in userbot_fleet_manager.clients.items():
                    if client_val is cl:
                        c_id = uid_key
                        break
                if not c_id:
                    c_id = cl._me.id if hasattr(cl, '_me') and cl._me else None
                if not c_id:
                    continue
                    
                is_selected = c_id in selected_ubs
                checkbox = "✅" if is_selected else "⬜"
                
                me = getattr(cl, '_me', None)
                first_name = me.first_name if me else "Userbot"
                username = f" @{me.username}" if (me and me.username) else f" (ID: {c_id})"
                
                buttons.append([Button.inline(f"{checkbox} {first_name}{username}", f"gm_tasktglub_{t_id}_{c_id}")])
            
            buttons.append([Button.inline("🔙 Done / Back", f"gm_task_view_{t_id}")])
            await event.edit(f"👤 **Select Userbots for task `{task[1]}` (Multiple selection enabled):**", buttons=buttons)
            await event.answer()

        elif data.startswith("gm_tasktglub_"):
            t_id = int(parts[2])
            ub_id = int(parts[3])
            
            task = db_get_task(t_id)
            selected_ubs = json.loads(task[2] or "[]")
            if ub_id in selected_ubs:
                selected_ubs.remove(ub_id)
            else:
                selected_ubs.append(ub_id)
                
            db_update_task(t_id, "userbot_ids", json.dumps(selected_ubs))
            await event.answer("Preference updated!")
            
            # Re-render userbots list
            task = db_get_task(t_id)
            clients = userbot_fleet_manager.get_all_clients()
            connected_clients = [c for c in clients if c.is_connected()]
            buttons = []
            for cl in connected_clients:
                c_id = None
                for uid_key, client_val in userbot_fleet_manager.clients.items():
                    if client_val is cl:
                        c_id = uid_key
                        break
                if not c_id:
                    c_id = cl._me.id if hasattr(cl, '_me') and cl._me else None
                if not c_id:
                    continue
                is_selected = c_id in selected_ubs
                checkbox = "✅" if is_selected else "⬜"
                
                me = getattr(cl, '_me', None)
                first_name = me.first_name if me else "Userbot"
                username = f" @{me.username}" if (me and me.username) else f" (ID: {c_id})"
                buttons.append([Button.inline(f"{checkbox} {first_name}{username}", f"gm_tasktglub_{t_id}_{c_id}")])
            buttons.append([Button.inline("🔙 Done / Back", f"gm_task_view_{t_id}")])
            await event.edit(f"👤 **Select Userbots for task `{task[1]}` (Multiple selection enabled):**", buttons=buttons)

        elif data.startswith("gm_taskmsg_"):
            t_id = int(parts[-1])
            admin_states[sender_id] = f"awaiting_gm_taskmsg_telethon_{t_id}"
            await event.reply("💬 **SET MAILER MESSAGE**\n\nPlease send the message you want to broadcast. It can contain text, photo, video, or document:")
            await event.answer()

        elif data.startswith("gm_taskgrps_"):
            t_id = int(parts[-1])
            await show_task_groups_page_telethon_helper(client, event, t_id, page=0)
            await event.answer()

        elif data.startswith("gm_tasksrcgrps_"):
            t_id = int(parts[-1])
            task = db_get_task(t_id)
            selected_ubs = json.loads(task[2] or "[]")
            if not selected_ubs:
                await event.answer("❌ Please select a userbot first!", alert=True)
                return
            primary_ub = str(selected_ubs[0])
            target_client = userbot_fleet_manager.get_client(int(primary_ub))
            if not target_client or not target_client.is_connected():
                await event.answer("❌ Selected userbot is offline!", alert=True)
                return
                
            await event.answer("🔄 Loading source chats...")
            await event.edit("⏳ *Fetching source chats list... Please wait...*")
            if primary_ub not in userbot_groups_cache:
                await fetch_dialogs_async(target_client, primary_ub)
            await show_task_sources_page_telethon_helper(client, event, t_id, page=0)

        elif data.startswith("gm_tasktglmode_"):
            t_id = int(parts[-1])
            task = db_get_task(t_id)
            new_mode = 0 if task[11] else 1
            db_update_task(t_id, "media_enabled", new_mode)
            await event.answer(f"Mode toggled to {'Media Fetcher' if new_mode else 'Static Message'}")
            task = db_get_task(t_id)
            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.edit(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)

        elif data.startswith("gm_tasktglmix_"):
            t_id = int(parts[-1])
            task = db_get_task(t_id)
            new_mix = 0 if task[10] else 1
            db_update_task(t_id, "media_mix", new_mix)
            await event.answer(f"Mix Mode toggled to {'ON' if new_mix else 'OFF'}")
            task = db_get_task(t_id)
            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.edit(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)

        elif data.startswith("gm_tpage_"):
            t_id = int(parts[2])
            page = int(parts[3])
            await show_task_groups_page_telethon_helper(client, event, t_id, page)
            await event.answer()

        elif data.startswith("gm_ttg_"):
            t_id = int(parts[2])
            g_id = int(parts[3])
            page = int(parts[4])
            
            task = db_get_task(t_id)
            selected_ids = json.loads(task[3] or "[]")
            if g_id in selected_ids:
                selected_ids.remove(g_id)
            else:
                selected_ids.append(g_id)
                
            db_update_task(t_id, "group_ids", json.dumps(selected_ids))
            await show_task_groups_page_telethon_helper(client, event, t_id, page)
            await event.answer()

        elif data.startswith("gm_tsrc_"):
            t_id = int(parts[2])
            g_id = int(parts[3])
            page = int(parts[4])
            
            task = db_get_task(t_id)
            selected_sources = json.loads(task[8] or "[]")
            if g_id in selected_sources:
                selected_sources.remove(g_id)
            else:
                selected_sources.append(g_id)
                
            db_update_task(t_id, "media_sources", json.dumps(selected_sources))
            await show_task_sources_page_telethon_helper(client, event, t_id, page)
            await event.answer()

        elif data.startswith("gm_tsrcpage_"):
            t_id = int(parts[2])
            page = int(parts[3])
            await show_task_sources_page_telethon_helper(client, event, t_id, page)
            await event.answer()

        elif data.startswith("gm_tselall_"):
            t_id = int(parts[2])
            page = int(parts[3])
            task = db_get_task(t_id)
            selected_ubs = json.loads(task[2] or "[]")
            primary_ub = str(selected_ubs[0])
            groups = userbot_groups_cache.get(primary_ub, [])
            selected_ids = set(json.loads(task[3] or "[]"))
            for g in groups:
                selected_ids.add(g["id"])
            db_update_task(t_id, "group_ids", json.dumps(list(selected_ids)))
            await show_task_groups_page_telethon_helper(client, event, t_id, page)
            await event.answer("Selected all groups!")

        elif data.startswith("gm_tsrcselall_"):
            t_id = int(parts[2])
            page = int(parts[3])
            task = db_get_task(t_id)
            selected_ubs = json.loads(task[2] or "[]")
            primary_ub = str(selected_ubs[0])
            groups = userbot_groups_cache.get(primary_ub, [])
            selected_sources = set(json.loads(task[8] or "[]"))
            for g in groups:
                selected_sources.add(g["id"])
            db_update_task(t_id, "media_sources", json.dumps(list(selected_sources)))
            await show_task_sources_page_telethon_helper(client, event, t_id, page)
            await event.answer("Selected all source chats!")

        elif data.startswith("gm_tclrall_"):
            t_id = int(parts[2])
            page = int(parts[3])
            db_update_task(t_id, "group_ids", "[]")
            await show_task_groups_page_telethon_helper(client, event, t_id, page)
            await event.answer("Cleared selections!")

        elif data.startswith("gm_tsrcclrall_"):
            t_id = int(parts[2])
            page = int(parts[3])
            db_update_task(t_id, "media_sources", "[]")
            await show_task_sources_page_telethon_helper(client, event, t_id, page)
            await event.answer("Cleared source selections!")

        elif data.startswith("gm_tref_"):
            t_id = int(parts[2])
            page = int(parts[3])
            task = db_get_task(t_id)
            selected_ubs = json.loads(task[2] or "[]")
            primary_ub = str(selected_ubs[0])
            
            target_client = userbot_fleet_manager.get_client(int(primary_ub))
            if not target_client or not target_client.is_connected():
                await event.answer("❌ Selected userbot is offline!", alert=True)
                return
                
            await event.answer("🔄 Syncing new groups...")
            await event.edit("🔄 *Syncing new groups from Telegram... Please wait...*")
            
            if primary_ub in userbot_groups_cache:
                del userbot_groups_cache[primary_ub]
                
            await fetch_dialogs_async(target_client, primary_ub)
            await show_task_groups_page_telethon_helper(client, event, t_id, page)

        elif data.startswith("gm_tsrcref_"):
            t_id = int(parts[2])
            page = int(parts[3])
            task = db_get_task(t_id)
            selected_ubs = json.loads(task[2] or "[]")
            primary_ub = str(selected_ubs[0])
            
            target_client = userbot_fleet_manager.get_client(int(primary_ub))
            if not target_client or not target_client.is_connected():
                await event.answer("❌ Selected userbot is offline!", alert=True)
                return
                
            await event.answer("🔄 Syncing source chats...")
            await event.edit("🔄 *Syncing source chats from Telegram... Please wait...*")
            
            if primary_ub in userbot_groups_cache:
                del userbot_groups_cache[primary_ub]
                
            await fetch_dialogs_async(target_client, primary_ub)
            await show_task_sources_page_telethon_helper(client, event, t_id, page)

        elif data.startswith("gm_tsetgrp_"):
            t_id = int(parts[2])
            page = int(parts[3])
            admin_states[sender_id] = f"awaiting_gm_tasklog_telethon_{t_id}_{page}"
            await event.reply("📢 **SET UPDATE/LOG GROUP FOR THIS TASK**\n\nPlease send the Group Chat ID (e.g. `-1001234567890`) where updates and failure logs should go.")
            await event.answer()

        elif data.startswith("gm_tdelgrp_"):
            t_id = int(parts[2])
            page = int(parts[3])
            db_update_task(t_id, "update_group_id", "")
            await event.answer("Log group removed!")
            await show_task_groups_page_telethon_helper(client, event, t_id, page)

        elif data.startswith("gm_tasklinks_"):
            t_id = int(parts[-1])
            admin_states[sender_id] = f"awaiting_gm_tasklinks_telethon_{t_id}"
            await event.reply("🔗 **IMPORT TASK GROUP JOIN LINKS**\n\nPlease send your group links (invite links or usernames, one per line).")
            await event.answer()

        elif data.startswith("gm_taskrep_"):
            t_id = int(parts[-1])
            buttons = [
                [Button.inline("❌ Off (Manual)", f"gm_tasksetrep_{t_id}_0"), Button.inline("30 Min", f"gm_tasksetrep_{t_id}_30")],
                [Button.inline("1 Hour", f"gm_tasksetrep_{t_id}_60"), Button.inline("2 Hours", f"gm_tasksetrep_{t_id}_120")],
                [Button.inline("6 Hours", f"gm_tasksetrep_{t_id}_360"), Button.inline("12 Hours", f"gm_tasksetrep_{t_id}_720")],
                [Button.inline("24 Hours", f"gm_tasksetrep_{t_id}_1440")],
                [Button.inline("🔙 Back", f"gm_task_view_{t_id}")]
            ]
            await event.edit("⏰ **SELECT REPEAT INTERVAL**\nConfigure how often this campaign task should automatically broadcast:", buttons=buttons)
            await event.answer()

        elif data.startswith("gm_tasksetrep_"):
            t_id = int(parts[2])
            minutes = int(parts[3])
            db_update_task(t_id, "repeat_interval", minutes)
            await event.answer("✅ Repeat interval updated!")
            
            task = db_get_task(t_id)
            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.edit(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)

        elif data.startswith("gm_taskmedint_"):
            t_id = int(parts[-1])
            buttons = [
                [Button.inline("2 Sec", f"gm_tasksetmedint_{t_id}_2"), Button.inline("5 Sec", f"gm_tasksetmedint_{t_id}_5")],
                [Button.inline("10 Sec", f"gm_tasksetmedint_{t_id}_10"), Button.inline("15 Sec", f"gm_tasksetmedint_{t_id}_15")],
                [Button.inline("20 Sec", f"gm_tasksetmedint_{t_id}_20"), Button.inline("30 Sec", f"gm_tasksetmedint_{t_id}_30")],
                [Button.inline("🔙 Back", f"gm_task_view_{t_id}")]
            ]
            await event.edit("⏱ **SELECT MEDIA INTERVAL**\nConfigure the delay in seconds between each media message being sent:", buttons=buttons)
            await event.answer()

        elif data.startswith("gm_tasksetmedint_"):
            t_id = int(parts[2])
            seconds = int(parts[3])
            db_update_task(t_id, "media_interval", seconds)
            await event.answer(f"✅ Media interval updated to {seconds}s!")
            task = db_get_task(t_id)
            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.edit(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)

        elif data.startswith("gm_taskdel_"):
            t_id = int(parts[-1])
            db_delete_task(t_id)
            await event.answer("🗑 Task deleted successfully!")
            
            # Re-render tasks list
            tasks = db_get_tasks()
            if not tasks:
                buttons = [
                    [Button.inline("➕ Create Task", "gm_tasks_create")],
                    [Button.inline("🔙 Back", "gm_tasks_main")]
                ]
                await event.edit("📋 **Group Mailer Tasks:** No tasks defined yet.", buttons=buttons)
                return
            buttons = []
            for t_id, name, interval, last_run in tasks:
                rep_lbl = "Manual" if interval == 0 else (f"{interval}m" if interval < 60 else f"{interval//60}h")
                buttons.append([Button.inline(f"📋 {name} ({rep_lbl})", f"gm_task_view_{t_id}")])
            buttons.append([Button.inline("🔙 Back", "gm_tasks_main")])
            await event.edit("📋 **Group Mailer Tasks:** Select a task to configure/run:", buttons=buttons)

        elif data.startswith("gm_taskstart_"):
            t_id = int(parts[-1])
            task = db_get_task(t_id)
            selected_ubs = json.loads(task[2] or "[]")
            selected_groups = json.loads(task[3] or "[]")
            msg_data = json.loads(task[4] or "{}")
            media_sources = json.loads(task[8] or "[]")
            media_enabled = task[11]

            if not selected_ubs:
                await event.answer("❌ Please select at least one Userbot first!", alert=True)
                return
            if not selected_groups:
                await event.answer("❌ Please select target groups first!", alert=True)
                return
                
            if media_enabled:
                if not media_sources:
                    await event.answer("❌ Please select media source chats first!", alert=True)
                    return
            else:
                if not msg_data:
                    await event.answer("❌ Please set the mailer message first!", alert=True)
                    return

            await event.answer("🚀 Starting campaign operation...")
            asyncio.create_task(run_task_broadcast(t_id))

        elif data.startswith("gm_taskstop_"):
            t_id = int(parts[-1])
            active_broadcasts[t_id] = False
            await event.answer("🛑 Stopping campaign operation...")
            task = db_get_task(t_id)
            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.edit(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)

        elif data.startswith("gm_tasktgldup_"):
            t_id = int(parts[-1])
            task = db_get_task(t_id)
            new_dedup = 0 if (task[13] if len(task) > 13 else 0) else 1
            db_update_task(t_id, "media_dedup", new_dedup)
            await event.answer(f"🛡️ Duplicate Protection: {'ON' if new_dedup else 'OFF'}")
            task = db_get_task(t_id)
            status_desc = get_task_status_text(task)
            buttons = get_task_control_buttons_telethon(task)
            await event.edit(f"📬 **TASK CONTROL PANEL**\n\n{status_desc}", buttons=buttons)

# Monkeypatch setup_automation_handlers to register our custom command handlers
original_setup_automation_handlers = main_module.setup_automation_handlers

def new_setup_automation_handlers(client):
    original_setup_automation_handlers(client)
    setup_group_mailer_handlers(client)

main_module.setup_automation_handlers = new_setup_automation_handlers

# Apply to any already connected clients in fleet
for client in userbot_fleet_manager.get_all_clients():
    if client.is_connected():
        setup_group_mailer_handlers(client)
