import sys
import os
import asyncio
import logging
import json
import re
from telethon import events, types, errors

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
is_authorized_manager = main_module.is_authorized_manager
get_setting = main_module.get_setting
ADMIN_ID = main_module.ADMIN_ID
userbot_fleet_manager = main_module.userbot_fleet_manager
loop = main_module.loop
parse_telegram_link = main_module.parse_telegram_link
db_conn = main_module.db_conn
get_placeholder = main_module.get_placeholder

_main_bot_username = None

async def get_main_bot_username():
    global _main_bot_username
    if _main_bot_username:
        return _main_bot_username
    try:
        me = await asyncio.get_event_loop().run_in_executor(None, bot.get_me)
        if me and me.username:
            _main_bot_username = me.username.replace("@", "")
            return _main_bot_username
    except Exception as e:
        logger.error(f"Error getting main bot username: {e}")
    return None

def get_vault_bots_from_db():
    vault_bots = []
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT bot_username FROM log_bots")
            for row in c.fetchall():
                if row[0]:
                    vault_bots.append(row[0].strip().replace("@", ""))
    except Exception as e:
        logger.error(f"Error fetching vault bots: {e}")
    return vault_bots

def get_managers_from_db():
    managers = []
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM managers")
            for row in c.fetchall():
                try:
                    managers.append(int(row[0]))
                except (ValueError, TypeError):
                    pass
    except Exception as e:
        logger.error(f"Error fetching managers from DB: {e}")
    
    if ADMIN_ID:
        try:
            managers.append(int(ADMIN_ID))
        except (ValueError, TypeError):
            pass
            
    return list(set(managers))

def get_target_pairs_from_db():
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT id, source_title, target_title, is_live, is_mirror FROM target_pairs")
            return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching target pairs: {e}")
        return []

def generate_tasks_report():
    pairs = get_target_pairs_from_db()
    if not pairs:
        return "📭 **No target pairs configured.**"
        
    lines = ["⚙️ **System Tasks Status Report**\n"]
    
    for pid, s_title, t_title, is_live, is_mirror in pairs:
        task_key = f"coll_{pid}"
        is_coll = main_module.running_tasks.get(task_key, False)
        
        job_type = "Stopped/Idle"
        details = ""
        
        if is_coll:
            opts = main_module.collection_options.get(task_key, {})
            scanned = opts.get("scanned", 0)
            collected = opts.get("collected", 0)
            status = opts.get("status", "Processing")
            progress = opts.get("progress", 0)
            
            # Progress bar (10 segments)
            bar_len = 10
            filled = int(round((progress / 100.0) * bar_len))
            bar = "█" * filled + "░" * (bar_len - filled)
            
            job_type = "Collecting + Mirroring" if is_mirror else "Collection"
            details = f"\n   • **Progress:** `[{bar}] {progress}%`\n   • **Stats:** Scanned: `{scanned}` | Collected: `{collected}`\n   • **Status:** `{status}`"
        elif is_live:
            job_type = "Live Forwarding + Mirroring" if is_mirror else "Live Forwarding"
            
        lines.append(
            f"⚡ **Pair #{pid}:** `{s_title}` ➡️ `{t_title}`\n"
            f"   • **Job:** `{job_type}`{details}\n"
        )
        
    return "\n".join(lines)

def build_telebot_markup():
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("⏸ Pause All", callback_data="task_pause_all"),
        InlineKeyboardButton("▶️ Restart All", callback_data="task_restart_all")
    )
    markup.row(
        InlineKeyboardButton("🔄 Refresh", callback_data="task_refresh")
    )
    return markup

def build_telethon_markup():
    from telethon import Button
    return [
        [Button.inline("⏸ Pause All", b"task_pause_all"), Button.inline("▶️ Restart All", b"task_restart_all")],
        [Button.inline("🔄 Refresh", b"task_refresh")]
    ]

def pause_all_tasks():
    pairs = get_target_pairs_from_db()
    paused_live = []
    paused_coll = []
    
    for pid, s_title, t_title, is_live, is_mirror in pairs:
        task_key = f"coll_{pid}"
        if main_module.running_tasks.get(task_key, False):
            paused_coll.append(pid)
            main_module.running_tasks[task_key] = False
            
        if is_live:
            paused_live.append(pid)
            try:
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    c.execute(f"UPDATE target_pairs SET is_live = 0 WHERE id = {p}", (pid,))
            except Exception as e:
                logger.error(f"Failed to stop live forwarding for pair {pid}: {e}")
                
    state = {
        "paused_live": paused_live,
        "paused_coll": paused_coll
    }
    main_module.set_setting("alljoin_paused_tasks_state", json.dumps(state))
    return len(paused_live) + len(paused_coll)

def restart_all_tasks():
    state_str = main_module.get_setting("alljoin_paused_tasks_state", "{}")
    try:
        state = json.loads(state_str)
    except Exception:
        state = {}
        
    paused_live = state.get("paused_live", [])
    paused_coll = state.get("paused_coll", [])
    
    restarted_count = 0
    
    for pid in paused_live:
        try:
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                c.execute(f"UPDATE target_pairs SET is_live = 1 WHERE id = {p}", (pid,))
            restarted_count += 1
        except Exception as e:
            logger.error(f"Failed to restart live forwarding for pair {pid}: {e}")
            
    for pid in paused_coll:
        try:
            asyncio.run_coroutine_threadsafe(
                main_module.run_collection(main_module.ADMIN_ID, pid),
                main_module.loop
            )
            restarted_count += 1
        except Exception as e:
            logger.error(f"Failed to restart collection for pair {pid}: {e}")
            
    main_module.set_setting("alljoin_paused_tasks_state", "{}")
    return restarted_count

# Auto-Rejoin Monitored Links Storage
def get_monitored_links():
    try:
        data = main_module.get_setting("autorejoin_links", "[]")
        return json.loads(data)
    except Exception:
        return []

def save_monitored_links(links):
    main_module.set_setting("autorejoin_links", json.dumps(links))

async def run_rejoin_check():
    links = get_monitored_links()
    if not links:
        return {}
        
    clients = userbot_fleet_manager.get_all_clients()
    if not clients:
        return {}

    results = {}
    for link in links:
        parsed = parse_telegram_link(link)
        results[link] = []
        
        for u_client in clients:
            if not u_client.is_connected():
                continue
                
            u_me = getattr(u_client, "_me", None)
            if not u_me:
                try:
                    u_me = await u_client.get_me()
                except Exception:
                    continue
                
            u_name = f"{u_me.first_name} (@{u_me.username})" if u_me.username else u_me.first_name

            try:
                if parsed and parsed["type"] == "invite":
                    from telethon.tl.functions.messages import ImportChatInviteRequest
                    try:
                        await u_client(ImportChatInviteRequest(parsed["hash"]))
                        results[link].append(f"✅ {u_name} joined/rejoined successfully.")
                    except errors.UserAlreadyParticipantError:
                        results[link].append(f"🟢 {u_name} is already a member.")
                else:
                    from telethon.tl.functions.channels import JoinChannelRequest
                    username = parsed["username"] if parsed else link.replace("@", "").strip()
                    chat_entity = await u_client.get_entity(username)
                    await u_client(JoinChannelRequest(chat_entity))
                    results[link].append(f"✅ {u_name} joined/rejoined successfully.")
            except Exception as je:
                err_str = str(je)
                if "USER_ALREADY_PARTICIPANT" in err_str:
                    results[link].append(f"🟢 {u_name} is already a member.")
                else:
                    results[link].append(f"❌ {u_name} failed to join: {err_str}")
                
    return results

async def autorejoin_check_loop():
    await asyncio.sleep(30) # Delay initial check on startup
    while True:
        try:
            await run_rejoin_check()
        except Exception as e:
            logger.error(f"Error in autorejoin loop: {e}")
        await asyncio.sleep(300) # Check every 5 minutes

def setup_alljoin_plugin_handlers(client):
    if getattr(client, '_alljoin_handlers_registered', False):
        return
    client._alljoin_handlers_registered = True

    @client.on(events.NewMessage(incoming=True))
    async def alljoin_plugin_handler(event):
        m = event.message
        if not m or not m.text:
            return

        if not event.is_private:
            return

        if not hasattr(client, '_me') or not client._me:
            try:
                client._me = await client.get_me()
            except Exception as e:
                logger.error(f"Failed to get_me() for userbot in alljoin plugin: {e}")

        me = getattr(client, '_me', None)

        # Enforce strict authorization checks for command usage
        text = m.text.strip()
        if text.startswith('.'):
            is_primary_admin = (m.sender_id == ADMIN_ID) or (me and m.sender_id == me.id)
            is_manager = is_primary_admin or is_authorized_manager(m.sender_id)
            
            if not is_manager:
                return

            parts = text.split()
            cmd = parts[0].lower()

            if cmd == '.alljoin':
                if len(parts) < 2:
                    await event.reply("❌ **Usage:** `.alljoin <link_or_username>`")
                    return

                target_link = parts[1].strip()
                await event.reply("⏳ **Initiating join process for all userbots in fleet...**")

                parsed = parse_telegram_link(target_link)
                clients = userbot_fleet_manager.get_all_clients()

                if not clients:
                    await event.reply("❌ No active userbots in the fleet.")
                    return

                results = []
                for u_client in clients:
                    if not u_client.is_connected():
                        continue
                    u_me = getattr(u_client, '_me', None)
                    if not u_me:
                        try:
                            u_me = await u_client.get_me()
                        except Exception:
                            continue

                    u_name = f"{u_me.first_name} (@{u_me.username})" if u_me.username else u_me.first_name

                    try:
                        chat_entity = None
                        already_joined = False
                        if parsed and parsed["type"] == "invite":
                            from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
                            
                            invite_info = None
                            try:
                                invite_info = await u_client(CheckChatInviteRequest(parsed["hash"]))
                                chat_entity = getattr(invite_info, 'chat', None)
                            except Exception as e:
                                logger.error(f"CheckChatInviteRequest failed for hash {parsed['hash']}: {e}")
                            
                            try:
                                result = await u_client(ImportChatInviteRequest(parsed["hash"]))
                                if hasattr(result, "chats") and result.chats:
                                    chat_entity = result.chats[0]
                            except errors.UserAlreadyParticipantError:
                                already_joined = True
                                if not chat_entity:
                                    if invite_info:
                                        chat_entity = getattr(invite_info, 'chat', None)
                                    else:
                                        try:
                                            invite_info = await u_client(CheckChatInviteRequest(parsed["hash"]))
                                            chat_entity = getattr(invite_info, 'chat', None)
                                        except Exception:
                                            pass
                            except Exception as e:
                                if "USER_ALREADY_PARTICIPANT" in str(e):
                                    already_joined = True
                                    if not chat_entity:
                                        if invite_info:
                                            chat_entity = getattr(invite_info, 'chat', None)
                                        else:
                                            try:
                                                invite_info = await u_client(CheckChatInviteRequest(parsed["hash"]))
                                                chat_entity = getattr(invite_info, 'chat', None)
                                            except Exception:
                                                pass
                                else:
                                    raise e
                                    
                            if not chat_entity and invite_info and hasattr(invite_info, 'chat') and invite_info.chat:
                                try:
                                    chat_entity = await u_client.get_entity(invite_info.chat.id)
                                except Exception:
                                    chat_entity = invite_info.chat
                        else:
                            from telethon.tl.functions.channels import JoinChannelRequest
                            username = parsed["username"] if parsed else target_link.replace("@", "")
                            chat_entity = await u_client.get_entity(username)
                            await u_client(JoinChannelRequest(chat_entity))

                        if chat_entity:
                            from telethon.utils import get_peer_id
                            cid = get_peer_id(chat_entity)
                            title = getattr(chat_entity, 'title', None) or getattr(chat_entity, 'first_name', None) or str(cid)
                            results.append({
                                "name": u_name,
                                "status": "Success",
                                "title": title,
                                "cid": cid
                            })
                        elif already_joined:
                            results.append({
                                "name": u_name,
                                "status": "Success",
                                "title": "Already Participant",
                                "cid": "Already Joined"
                            })
                        else:
                            results.append({
                                "name": u_name,
                                "status": "Failed to retrieve chat entity",
                                "title": None,
                                "cid": None
                            })
                    except Exception as e:
                        err_str = str(e)
                        if "USER_ALREADY_PARTICIPANT" in err_str:
                            from telethon.utils import get_peer_id
                            cid = get_peer_id(chat_entity) if 'chat_entity' in locals() and chat_entity else "Already Joined"
                            title = getattr(chat_entity, 'title', None) or getattr(chat_entity, 'first_name', None) or "Already Participant" if 'chat_entity' in locals() and chat_entity else "Already Participant"
                            results.append({
                                "name": u_name,
                                "status": "Success",
                                "title": title,
                                "cid": cid
                            })
                        else:
                            results.append({
                                "name": u_name,
                                "status": f"Failed: {str(e)}",
                                "title": None,
                                "cid": None
                            })

                # Format report
                report_lines = ["📢 **AllJoin Request Results:**"]
                for res in results:
                    if res['status'] == "Success":
                        report_lines.append(f"👤 **{res['name']}**: ✅ Joined `{res['title']}` (`{res['cid']}`)")
                    else:
                        report_lines.append(f"👤 **{res['name']}**: ❌ Failed: `{res['status']}`")

                await event.reply("\n".join(report_lines))
                return

            elif cmd == '.id':
                if len(parts) < 2:
                    await event.reply("❌ **Usage:** `.id <group_name_or_username>`")
                    return

                search_query = parts[1].strip()
                await event.reply(f"⏳ **Searching for '{search_query}'...**")

                matches = []
                try:
                    async for dialog in client.iter_dialogs():
                        title = dialog.name or ""
                        username = getattr(dialog.entity, 'username', None) or ""
                        d_id = dialog.id

                        if (search_query.lower() in title.lower()) or (username and search_query.lower() in username.lower()):
                            matches.append((title, username, d_id))
                            if len(matches) >= 10:
                                break
                except Exception as e:
                    logger.error(f"Error searching dialogs in plugin: {e}")

                if not matches:
                    # Try fallback resolve
                    try:
                        entity = await client.get_entity(search_query)
                        if entity:
                            from telethon.utils import get_peer_id
                            cid = get_peer_id(entity)
                            title = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(cid)
                            username = getattr(entity, 'username', None) or ""
                            matches.append((title, username, cid))
                    except Exception:
                        pass

                if matches:
                    report_lines = [f"🔍 **Search Results for '{search_query}':**"]
                    for title, username, d_id in matches:
                        uname_str = f" (@{username})" if username else ""
                        report_lines.append(f"• **Name:** `{title}`{uname_str}\n  **ID:** `{d_id}`")
                    await event.reply("\n".join(report_lines))
                else:
                    await event.reply(f"❌ No matching groups or chats found for '{search_query}'.")
                return

            elif cmd == '.synccontacts':
                await event.reply("⏳ **Syncing contacts across all userbots...**")
                
                clients = userbot_fleet_manager.get_all_clients()
                if not clients:
                    await event.reply("❌ No active userbots in the fleet.")
                    return

                userbot_infos = []
                for u_client in clients:
                    if not u_client.is_connected():
                        continue
                    try:
                        u_me = getattr(u_client, '_me', None)
                        if not u_me:
                            u_me = await u_client.get_me()
                        if u_me:
                            userbot_infos.append(u_me)
                    except Exception as e:
                        logger.error(f"Failed to fetch userbot profile for contact sync: {e}")

                manager_ids = get_managers_from_db()
                report_lines = ["📢 **Contacts Sync Results:**"]

                for u_client in clients:
                    if not u_client.is_connected():
                        continue
                    
                    u_me = getattr(u_client, '_me', None)
                    if not u_me:
                        try:
                            u_me = await u_client.get_me()
                        except Exception:
                            continue

                    u_name = f"{u_me.first_name} (@{u_me.username})" if u_me.username else u_me.first_name
                    success_count = 0
                    fail_count = 0

                    for info in userbot_infos:
                        if info.id == u_me.id:
                            continue
                        try:
                            from telethon.tl.functions.contacts import AddContactRequest
                            await u_client(AddContactRequest(
                                id=info.id,
                                first_name=info.first_name or f"Userbot_{info.id}",
                                last_name=info.last_name or "",
                                phone=info.phone or "",
                                add_phone_privacy_exception=True
                            ))
                            success_count += 1
                        except Exception as e:
                            logger.error(f"Userbot {u_me.id} failed to add userbot {info.id}: {e}")
                            fail_count += 1

                    for m_id in manager_ids:
                        if m_id == u_me.id:
                            continue
                        try:
                            entity = await u_client.get_entity(m_id)
                            from telethon.tl.functions.contacts import AddContactRequest
                            await u_client(AddContactRequest(
                                id=entity.id,
                                first_name=entity.first_name or f"Manager_{entity.id}",
                                last_name=entity.last_name or "",
                                phone=entity.phone or "",
                                add_phone_privacy_exception=True
                            ))
                            success_count += 1
                        except Exception as e:
                            logger.error(f"Userbot {u_me.id} failed to add manager {m_id}: {e}")
                            fail_count += 1

                    report_lines.append(f"👤 **Userbot:** `{u_name}`\n   ✅ Added: `{success_count}` | ❌ Failed: `{fail_count}`")

                await event.reply("\n".join(report_lines))
                return

            elif cmd in ['.tasks', '.task']:
                text = generate_tasks_report()
                await event.reply(text, buttons=build_telethon_markup(), parse_mode="Markdown")
                return

            elif cmd == '.mainbot':
                m_username = await get_main_bot_username()
                if m_username:
                    await event.reply(f"🤖 **Main Bot Username:** @{m_username}")
                else:
                    await event.reply("🤖 **Main Bot Username:** Not configured / failed to fetch.")
                return

            elif cmd == '.vaultbot':
                v_bots = get_vault_bots_from_db()
                if v_bots:
                    reply_list = [f"• @{bot}" for bot in v_bots]
                    await event.reply("🔑 **Vault Bot Username(s):**\n" + "\n".join(reply_list))
                else:
                    await event.reply("🔑 **Vault Bot Username(s):** No vault bots configured.")
                return

            elif cmd in ['.autorejoin', '.rejoin']:
                if len(parts) < 2:
                    await event.reply("❌ **Usage:**\n`.autorejoin add <link>`\n`.autorejoin del <link>`\n`.autorejoin list`\n`.autorejoin check`")
                    return
                subcmd = parts[1].lower()
                
                if subcmd == 'add':
                    if len(parts) < 3:
                        await event.reply("❌ **Usage:** `.autorejoin add <link>`")
                        return
                    new_link = parts[2].strip()
                    links = get_monitored_links()
                    if new_link not in links:
                        links.append(new_link)
                        save_monitored_links(links)
                        await event.reply(f"✅ Added `{new_link}` to auto-rejoin list. Initiating immediate check...")
                        asyncio.create_task(run_rejoin_check())
                    else:
                        await event.reply("ℹ️ This link is already on the auto-rejoin list.")
                        
                elif subcmd == 'del':
                    if len(parts) < 3:
                        await event.reply("❌ **Usage:** `.autorejoin del <link>`")
                        return
                    old_link = parts[2].strip()
                    links = get_monitored_links()
                    if old_link in links:
                        links.remove(old_link)
                        save_monitored_links(links)
                        await event.reply(f"✅ Removed `{old_link}` from auto-rejoin list.")
                    else:
                        await event.reply("❌ Link not found in the auto-rejoin list.")
                        
                elif subcmd == 'list':
                    links = get_monitored_links()
                    if links:
                        await event.reply("🔄 **Auto-Rejoin Monitored Links:**\n" + "\n".join([f"• {link}" for link in links]))
                    else:
                        await event.reply("📭 Auto-rejoin list is empty.")
                        
                elif subcmd == 'check':
                    await event.reply("⏳ Running membership validation and rejoin checks across all userbots...")
                    report = await run_rejoin_check()
                    if not report:
                        await event.reply("✅ Check complete. No monitored links or no active userbots.")
                        return
                    rep_lines = ["🔄 **Auto-Rejoin Validation Results:**"]
                    for link, u_statuses in report.items():
                        rep_lines.append(f"📂 **Link:** `{link}`")
                        for status in u_statuses:
                            rep_lines.append(f"  {status}")
                    await event.reply("\n".join(rep_lines))
                return

    @client.on(events.CallbackQuery(data=re.compile(b'^task_')))
    async def task_telethon_callback_handler(event):
        sender_id = event.sender_id
        if not hasattr(client, '_me') or not client._me:
            try:
                client._me = await client.get_me()
            except Exception:
                pass
        me = getattr(client, '_me', None)
        
        is_primary_admin = (sender_id == ADMIN_ID) or (me and sender_id == me.id)
        is_manager = is_primary_admin or is_authorized_manager(sender_id)
        if not is_manager:
            await event.answer("❌ Unauthorized", alert=True)
            return

        action = event.data.decode('utf-8')
        if action == "task_pause_all":
            count = pause_all_tasks()
            await event.answer(f"⏸ Paused {count} tasks.", alert=True)
            text = generate_tasks_report()
            await event.edit(text, buttons=build_telethon_markup(), parse_mode="Markdown")
            
        elif action == "task_restart_all":
            count = restart_all_tasks()
            await event.answer(f"▶️ Restarted {count} tasks.", alert=True)
            text = generate_tasks_report()
            await event.edit(text, buttons=build_telethon_markup(), parse_mode="Markdown")
            
        elif action == "task_refresh":
            await event.answer("🔄 Refreshed")
            text = generate_tasks_report()
            await event.edit(text, buttons=build_telethon_markup(), parse_mode="Markdown")

# Main admin bot (telebot) handlers
@bot.message_handler(commands=['tasks', 'task'])
def custom_bot_tasks(message):
    if not is_authorized_manager(message.from_user.id):
        return
    text = generate_tasks_report()
    bot.send_message(message.chat.id, text, reply_markup=build_telebot_markup(), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data.startswith('task_'))
def bot_task_callbacks(call):
    if not is_authorized_manager(call.from_user.id):
        bot.answer_callback_query(call.id, "❌ Unauthorized", show_alert=True)
        return
        
    action = call.data
    if action == "task_pause_all":
        count = pause_all_tasks()
        bot.answer_callback_query(call.id, f"⏸ Paused {count} tasks.")
        text = generate_tasks_report()
        bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=build_telebot_markup(), parse_mode="Markdown")
        
    elif action == "task_restart_all":
        count = restart_all_tasks()
        bot.answer_callback_query(call.id, f"▶️ Restarted {count} tasks.")
        text = generate_tasks_report()
        bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=build_telebot_markup(), parse_mode="Markdown")
        
    elif action == "task_refresh":
        bot.answer_callback_query(call.id, "🔄 Refreshed")
        text = generate_tasks_report()
        bot.edit_message_text(text, chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=build_telebot_markup(), parse_mode="Markdown")

@bot.message_handler(commands=['autorejoin', 'rejoin'])
def bot_autorejoin(message):
    if not is_authorized_manager(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "❌ **Usage:**\n`/autorejoin add <link>`\n`/autorejoin del <link>`\n`/autorejoin list`\n`/autorejoin check`", parse_mode="Markdown")
        return
    subcmd = parts[1].lower()
    
    if subcmd == 'add':
        if len(parts) < 3:
            bot.reply_to(message, "❌ **Usage:** `/autorejoin add <link>`")
            return
        new_link = parts[2].strip()
        links = get_monitored_links()
        if new_link not in links:
            links.append(new_link)
            save_monitored_links(links)
            bot.reply_to(message, f"✅ Added `{new_link}` to auto-rejoin list. Initiating immediate check...")
            asyncio.run_coroutine_threadsafe(run_rejoin_check(), loop)
        else:
            bot.reply_to(message, "ℹ️ This link is already on the auto-rejoin list.")
            
    elif subcmd == 'del':
        if len(parts) < 3:
            bot.reply_to(message, "❌ **Usage:** `/autorejoin del <link>`")
            return
        old_link = parts[2].strip()
        links = get_monitored_links()
        if old_link in links:
            links.remove(old_link)
            save_monitored_links(links)
            bot.reply_to(message, f"✅ Removed `{old_link}` from auto-rejoin list.")
        else:
            bot.reply_to(message, "❌ Link not found in the auto-rejoin list.")
            
    elif subcmd == 'list':
        links = get_monitored_links()
        if links:
            bot.reply_to(message, "🔄 **Auto-Rejoin Monitored Links:**\n" + "\n".join([f"• {link}" for link in links]), parse_mode="Markdown")
        else:
            bot.reply_to(message, "📭 Auto-rejoin list is empty.")
            
    elif subcmd == 'check':
        bot.reply_to(message, "⏳ Running membership validation and rejoin checks across all userbots...")
        
        async def run_check_and_reply():
            report = await run_rejoin_check()
            if not report:
                bot.send_message(message.chat.id, "✅ Check complete. No monitored links or no active userbots.")
                return
            rep_lines = ["🔄 **Auto-Rejoin Validation Results:**"]
            for link, u_statuses in report.items():
                rep_lines.append(f"📂 **Link:** `{link}`")
                for status in u_statuses:
                    rep_lines.append(f"  {status}")
            bot.send_message(message.chat.id, "\n".join(rep_lines), parse_mode="Markdown")
            
        asyncio.run_coroutine_threadsafe(run_check_and_reply(), loop)

# Start background check loop
asyncio.create_task(autorejoin_check_loop())

# Monkeypatch setup_automation_handlers to register our custom command handlers
original_setup_automation_handlers = main_module.setup_automation_handlers

def new_setup_automation_handlers(client):
    original_setup_automation_handlers(client)
    setup_alljoin_plugin_handlers(client)

main_module.setup_automation_handlers = new_setup_automation_handlers

# Apply to any already connected clients in fleet
for client in userbot_fleet_manager.get_all_clients():
    if client.is_connected():
        setup_alljoin_plugin_handlers(client)
