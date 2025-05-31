import logging
import os
import sys
import asyncio
import json
import subprocess # For executing system commands like restart, backup
from datetime import datetime

# Add parent directory to sys.path to allow importing from Function/ and Database/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler

# Import utility functions
# No longer need set_db_connector as transaction logs are in files
from Function.utils import load_config, setup_logging, generate_id_stamp, is_user_allowed, log_transaction_to_file, update_transaction_log_file_status
# Import database connector
from Database.db_connector import DatabaseConnector

# --- Global Variables & Initialization ---
BOT_ID = "bot4"
BOT_TOKEN = None
DB_CONNECTOR = None
ADMIN_SETTINGS = {}
USERS_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'Config', 'users.json')
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'Config', 'config.json')
LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'Log')

# Load configs and setup logging
try:
    config = load_config(CONFIG_PATH)
    tokens = load_config(os.path.join(os.path.dirname(__file__), '..', 'Config', 'token.json'))

    if not config or not tokens:
        logging.error("Failed to load config or tokens. Exiting.")
        sys.exit(1)

    BOT_TOKEN = tokens.get("BOT4_TOKEN")
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_4":
        logging.error("BOT4_TOKEN not found or is placeholder. Please update Config/token.json")
        sys.exit(1)

    # Setup logging for Bot4
    log_level = config.get("LOGGING_CONFIG", {}).get("LEVEL", "INFO").upper()
    logger = setup_logging(BOT_ID, log_level)
    logger.info(f"--- Starting Bot4: {BOT_ID} ---")

    # Initialize Database Connector (still needed for inventory and reservations tables)
    DB_CONNECTOR = DatabaseConnector()
    # No need to set_db_connector for utils as transaction logs are to file.

    try:
        DB_CONNECTOR.connect()
        logger.info("Bot4 successfully connected to database.")
    except Exception as e:
        logger.error(f"Bot4 failed to connect to database: {e}. Admin functionalities might be impaired.")

    ADMIN_SETTINGS = config.get("ADMIN_SETTINGS", {})

except Exception as e:
    logging.critical(f"Critical error during Bot4 initialization: {e}")
    sys.exit(1)

# --- Admin Specific Functions ---

def is_super_admin(user_id):
    """Checks if the user is a super admin."""
    users_data = load_config(USERS_CONFIG_PATH)
    if users_data:
        return user_id in users_data.get("super_admin", [])
    return False

async def update_user_permissions(bot_id_target, user_id_target, action, user_id_admin, username_admin, id_stamp, raw_command, parsed_details):
    """Adds or removes a user ID from a bot's allowed list."""
    overall_success = True
    error_message = None
    return_message = ""

    # Log initial state
    log_transaction_to_file(id_stamp, f"{action}_user", user_id_admin, username_admin, raw_command, parsed_details, "PROCESSING", f"Attempting to {action} user {user_id_target} to/from {bot_id_target}")

    try:
        users_data = load_config(USERS_CONFIG_PATH)
        if users_data is None:
            overall_success = False
            error_message = "ไม่สามารถโหลดข้อมูลผู้ใช้ได้."
            return_message = f"❌ {error_message}"
        else:
            if bot_id_target not in users_data and bot_id_target not in ['bot1', 'bot2', 'bot3', 'bot4', 'super_admin']:
                overall_success = False
                error_message = f"ไม่พบบอท ID '{bot_id_target}'. โปรดระบุ BotID ที่ถูกต้อง (bot1, bot2, bot3, bot4, super_admin)."
                return_message = f"❌ {error_message}"
            else:
                target_list = users_data.get(bot_id_target, [])
                
                if action == "add":
                    if user_id_target not in target_list:
                        target_list.append(user_id_target)
                        users_data[bot_id_target] = target_list
                        with open(USERS_CONFIG_PATH, 'w', encoding='utf-8') as f:
                            json.dump(users_data, f, indent=4)
                        logger.info(f"[{id_stamp}] Admin {username_admin} added user {user_id_target} to {bot_id_target}.")
                        return_message = f"✅ เพิ่ม User ID `{user_id_target}` ใน {bot_id_target} แล้ว."
                    else:
                        return_message = f"ℹ️ User ID `{user_id_target}` มีอยู่ใน {bot_id_target} แล้ว."
                elif action == "remove":
                    if user_id_target in target_list:
                        target_list.remove(user_id_target)
                        users_data[bot_id_target] = target_list
                        with open(USERS_CONFIG_PATH, 'w', encoding='utf-8') as f:
                            json.dump(users_data, f, indent=4)
                        logger.info(f"[{id_stamp}] Admin {username_admin} removed user {user_id_target} from {bot_id_target}.")
                        return_message = f"✅ ลบ User ID `{user_id_target}` ออกจาก {bot_id_target} แล้ว."
                    else:
                        return_message = f"ℹ️ User ID `{user_id_target}` ไม่พบใน {bot_id_target}."
                else:
                    overall_success = False
                    error_message = "การกระทำไม่ถูกต้อง (ต้องเป็น 'add' หรือ 'remove')."
                    return_message = f"❌ {error_message}"

    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดในการอัปเดตสิทธิ์ผู้ใช้: {e}"
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] Error updating user permissions: {e}")
    finally:
        final_status = "SUCCESS" if overall_success and not error_message else "FAILED"
        final_message = return_message.replace("✅ ", "").replace("ℹ️ ", "").replace("❌ ", "")
        update_transaction_log_file_status(id_stamp, final_status, final_message, error_details=error_message)
        return return_message


async def get_log_content(bot_id_target, user_id, username, id_stamp, raw_command, parsed_details):
    """Reads and returns content of a specific bot's log file."""
    overall_success = True
    error_message = None
    log_file_path = os.path.join(LOG_DIR, f"{bot_id_target}.log")
    return_content = ""

    # Log initial state
    log_transaction_to_file(id_stamp, "viewlogs", user_id, username, raw_command, parsed_details, "PROCESSING", f"Attempting to view logs for {bot_id_target}")

    if not os.path.exists(log_file_path):
        overall_success = False
        error_message = f"ไม่พบไฟล์ Log สำหรับบอท ID '{bot_id_target}'."
        return_content = f"❌ {error_message}"
    else:
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                return_content = "".join(lines[-50:]) # Send last 50 lines for display
            
            logger.info(f"[{id_stamp}] Admin {username} viewed logs for {bot_id_target}.")
        except Exception as e:
            overall_success = False
            error_message = f"เกิดข้อผิดพลาดในการอ่านไฟล์ Log: {e}"
            return_content = f"❌ {error_message}"
            logger.error(f"[{id_stamp}] Error reading log file {log_file_path}: {e}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = f"Viewed logs for {bot_id_target}" if overall_success else error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details=error_message)
    return return_content

async def get_system_config_content(user_id, username, id_stamp, raw_command, parsed_details):
    """Reads and returns current system configurations."""
    overall_success = True
    error_message = None
    config_str = ""

    # Log initial state
    log_transaction_to_file(id_stamp, "config", user_id, username, raw_command, parsed_details, "PROCESSING", "Attempting to view system config")

    try:
        config_data = load_config(CONFIG_PATH)
        if config_data:
            display_config = config_data.copy()
            if "DATABASE_CONFIG" in display_config and "PASSWORD" in display_config["DATABASE_CONFIG"]:
                display_config["DATABASE_CONFIG"]["PASSWORD"] = "****" # Mask password
            
            config_str = json.dumps(display_config, indent=2, ensure_ascii=False)
            logger.info(f"[{id_stamp}] Admin {username} viewed system configuration.")
        else:
            overall_success = False
            error_message = "ไม่สามารถโหลดการตั้งค่าระบบได้."
            config_str = f"❌ {error_message}"
    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดในการอ่านการตั้งค่า: {e}"
        config_str = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] Error reading system config: {e}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = "Viewed system config" if overall_success else error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details=error_message)
    return config_str

async def restart_bot_process(bot_id_target, user_id, username, id_stamp, raw_command, parsed_details):
    """Simulates restarting a bot process."""
    overall_success = True
    error_message = None
    restart_status_msg = ""

    # Log initial state
    log_transaction_to_file(id_stamp, "restart", user_id, username, raw_command, parsed_details, "PROCESSING", f"Attempting to restart {bot_id_target}")

    if bot_id_target not in ["bot1", "bot2", "bot3", "bot4", "all"]:
        overall_success = False
        error_message = f"ไม่พบบอท ID '{bot_id_target}' สำหรับการรีสตาร์ท. (bot1, bot2, bot3, bot4, all)"
        restart_status_msg = f"❌ {error_message}"
    else:
        # Placeholder for actual restart logic using a process manager
        restart_status_msg = f"⚙️ กำลังส่งคำสั่งรีสตาร์ทบอท '{bot_id_target}' (Transaction ID: {id_stamp})... (ฟังก์ชันนี้ต้องการการตั้งค่า Process Manager ภายนอก)"
        logger.info(f"[{id_stamp}] Admin {username} issued restart command for: {bot_id_target}. (Placeholder)")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = f"Issued restart command for {bot_id_target}" if overall_success else error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details=error_message)
    return restart_status_msg


async def backup_database(user_id, username, id_stamp, raw_command, parsed_details):
    """Executes a database backup using mysqldump."""
    overall_success = True
    error_message = None
    backup_file_name = None
    return_message = ""

    # Log initial state
    log_transaction_to_file(id_stamp, "backupdb", user_id, username, raw_command, parsed_details, "PROCESSING", "Attempting database backup")

    try:
        db_conf = config["DATABASE_CONFIG"]
        db_name = db_conf["DATABASE"]
        db_user = db_conf["USER"]
        db_password = db_conf["PASSWORD"]
        db_host = db_conf["HOST"]

        backup_dir = os.path.join(os.path.dirname(__file__), '..', 'Backup')
        os.makedirs(backup_dir, exist_ok=True)
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"{db_name}_backup_{timestamp_str}.sql")
        backup_file_name = os.path.basename(backup_file)

        command = [
            "mysqldump",
            f"--host={db_host}",
            f"--user={db_user}",
            f"--password={db_password}",
            db_name
        ]

        logger.info(f"[{id_stamp}] Admin {username} executing DB backup: {' '.join(command[:-1])} --password=****** {db_name}")
        
        with open(backup_file, 'w', encoding='utf-8') as f:
            process = subprocess.run(command, stdout=f, stderr=subprocess.PIPE, check=True, text=True)

        if process.stderr:
            logger.warning(f"[{id_stamp}] mysqldump stderr: {process.stderr}")
        
        logger.info(f"[{id_stamp}] Database backup successful to: {backup_file}")
        return_message = f"✅ สำรองฐานข้อมูลเรียบร้อยแล้ว: `{backup_file_name}` (Transaction ID: {id_stamp})"
    except FileNotFoundError:
        overall_success = False
        error_message = "'mysqldump' command not found. Make sure MySQL client tools are installed and in PATH."
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] {error_message}")
    except subprocess.CalledProcessError as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดในการสำรองฐานข้อมูล: {e.stderr}"
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] {error_message}")
    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดที่ไม่คาดคิดในการสำรองฐานข้อมูล: {e}"
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] {error_message}")
    finally:
        final_status = "SUCCESS" if overall_success else "FAILED"
        final_message = f"Database backup successful: {backup_file_name}" if overall_success else error_message
        update_transaction_log_file_status(id_stamp, final_status, final_message, error_details=error_message)
        return return_message


async def restore_database(backup_file_name, user_id, username, id_stamp, raw_command, parsed_details):
    """Executes a database restore from an SQL file."""
    overall_success = True
    error_message = None
    return_message = ""
    
    # Initial log for restore command is handled by handle_restoredb_command, setting status to PENDING_CONFIRMATION
    # This log will happen after confirmation
    log_transaction_to_file(id_stamp, "restoredb_confirmed", user_id, username, raw_command, parsed_details, "PROCESSING", f"Confirmation received. Starting database restore from {backup_file_name}")

    backup_dir = os.path.join(os.path.dirname(__file__), '..', 'Backup')
    backup_file_path = os.path.join(backup_dir, backup_file_name)

    if not os.path.exists(backup_file_path):
        overall_success = False
        error_message = f"ไม่พบไฟล์สำรอง: `{backup_file_name}` ในโฟลเดอร์ Backup."
        return_message = f"❌ {error_message}"
    else:
        db_conf = config["DATABASE_CONFIG"]
        db_name = db_conf["DATABASE"]
        db_user = db_conf["USER"]
        db_password = db_conf["PASSWORD"]
        db_host = db_conf["HOST"]

        command = [
            "mysql",
            f"--host={db_host}",
            f"--user={db_user}",
            f"--password={db_password}",
            db_name
        ]

        logger.warning(f"[{id_stamp}] Admin {username} attempting to restore database from: {backup_file_path}. THIS WILL OVERWRITE EXISTING DATA!")

        try:
            with open(backup_file_path, 'r', encoding='utf-8') as f:
                process = subprocess.run(command, stdin=f, stderr=subprocess.PIPE, check=True, text=True)

            if process.stderr:
                logger.warning(f"[{id_stamp}] mysql client stderr during restore: {process.stderr}")

            logger.info(f"[{id_stamp}] Database restore successful from: {backup_file_path}")
            return_message = f"✅ กู้คืนฐานข้อมูลจาก `{backup_file_name}` เรียบร้อยแล้ว. (Transaction ID: {id_stamp})"
        except FileNotFoundError:
            overall_success = False
            error_message = "'mysql' command not found. Make sure MySQL client tools are installed and in PATH."
            return_message = f"❌ {error_message}"
            logger.error(f"[{id_stamp}] {error_message}")
        except subprocess.CalledProcessError as e:
            overall_success = False
            error_message = f"เกิดข้อผิดพลาดในการกู้คืนฐานข้อมูล: {e.stderr}"
            return_message = f"❌ {error_message}"
            logger.error(f"[{id_stamp}] {error_message}")
        except Exception as e:
            overall_success = False
            error_message = f"เกิดข้อผิดพลาดที่ไม่คาดคิดในการกู้คืนฐานข้อมูล: {e}"
            return_message = f"❌ {error_message}"
            logger.error(f"[{id_stamp}] {error_message}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = f"Database restored from {backup_file_name}" if overall_success else error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details=error_message)
    return return_message


# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message when the command /start is issued."""
    id_stamp = generate_id_stamp("START")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    raw_command = update.message.text
    
    log_transaction_to_file(id_stamp, "start", user_id, username, raw_command, None, "PROCESSING", "Command received")
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /start command.")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานบอท {BOT_ID} นี้ กรุณาติดต่อผู้ดูแลระบบ. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt by user {user_id} for bot {BOT_ID}.")
        return

    is_sa = is_super_admin(user_id)
    admin_commands = (
        f"/adduser (BotID) (UserID)\n"
        f"/removeuser (BotID) (UserID)\n"
        f"/viewlogs (BotID)\n"
        f"/config\n"
    )
    if is_sa:
        admin_commands += (
            f"/restart (BotID)\n"
            f"/backupdb\n"
            f"/restoredb (FileName)\n"
        )
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", "Welcome message sent")
    await update.message.reply_text(
        f"สวัสดีครับ! ยินดีต้อนรับสู่ Bot4 (ระบบ Admin).\n"
        f"คุณสามารถใช้คำสั่งต่อไปนี้:\n"
        f"{admin_commands}"
        f"Transaction ID: {id_stamp}"
    )

async def handle_adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /adduser command."""
    id_stamp = generate_id_stamp("ADDUSR")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # Initial logging is within update_user_permissions

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /adduser command: {full_command}")

    if not is_super_admin(user_id):
        log_transaction_to_file(id_stamp, "adduser", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access (not super admin)")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. เฉพาะ Super Admin เท่านั้น. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized /adduser attempt by user {user_id}.")
        return

    args = context.args
    if len(args) != 2:
        log_transaction_to_file(id_stamp, "adduser", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /adduser (BotID) (UserID) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /adduser command format from user {user_id}. Args: {args}")
        return
    
    bot_id_target = args[0].lower()
    try:
        user_id_target = int(args[1])
    except ValueError:
        log_transaction_to_file(id_stamp, "adduser", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid UserID format", "UserID is not a valid integer.")
        await update.message.reply_text(f"❌ UserID ต้องเป็นตัวเลข. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Invalid UserID format for /adduser by user {user_id}.")
        return
    
    result = await update_user_permissions(bot_id_target, user_id_target, "add", user_id, username, id_stamp, full_command, {"bot_id": bot_id_target, "user_id_target": user_id_target})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /removeuser command."""
    id_stamp = generate_id_stamp("RMVUSR")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # Initial logging is within update_user_permissions

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /removeuser command: {full_command}")

    if not is_super_admin(user_id):
        log_transaction_to_file(id_stamp, "removeuser", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access (not super admin)")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. เฉพาะ Super Admin เท่านั้น. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized /removeuser attempt by user {user_id}.")
        return

    args = context.args
    if len(args) != 2:
        log_transaction_to_file(id_stamp, "removeuser", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /removeuser (BotID) (UserID) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /removeuser command format from user {user_id}. Args: {args}")
        return
    
    bot_id_target = args[0].lower()
    try:
        user_id_target = int(args[1])
    except ValueError:
        log_transaction_to_file(id_stamp, "removeuser", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid UserID format", "UserID is not a valid integer.")
        await update.message.reply_text(f"❌ UserID ต้องเป็นตัวเลข. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Invalid UserID format for /removeuser by user {user_id}.")
        return
    
    result = await update_user_permissions(bot_id_target, user_id_target, "remove", user_id, username, id_stamp, full_command, {"bot_id": bot_id_target, "user_id_target": user_id_target})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_viewlogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /viewlogs command."""
    id_stamp = generate_id_stamp("VWLOGS")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # Initial logging is within get_log_content

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /viewlogs command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID): # Check if user is allowed to use Admin bot
        log_transaction_to_file(id_stamp, "viewlogs", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized /viewlogs attempt by user {user_id}.")
        return

    args = context.args
    if len(args) != 1:
        log_transaction_to_file(id_stamp, "viewlogs", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /viewlogs (BotID) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /viewlogs command format from user {user_id}. Args: {args}")
        return
    
    bot_id_target = args[0].lower()
    log_content = await get_log_content(bot_id_target, user_id, username, id_stamp, full_command, {"bot_id": bot_id_target})
    
    await update.message.reply_text(
        f"Log สำหรับ '{bot_id_target}' (Transaction ID: {id_stamp}):\n"
        f"```\n{log_content}\n```",
        parse_mode='MarkdownV2'
    )
    logger.info(f"[{id_stamp}] User {user_id} viewed logs for {bot_id_target}.")

async def handle_config_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /config command."""
    id_stamp = generate_id_stamp("VIEWCFG")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # Initial logging is within get_system_config_content

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /config command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "config", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized /config attempt by user {user_id}.")
        return
    
    config_content = await get_system_config_content(user_id, username, id_stamp, full_command, None) # Log status updated inside get_system_config_content
    await update.message.reply_text(
        f"การตั้งค่าระบบ (Transaction ID: {id_stamp}):\n"
        f"```json\n{config_content}\n```",
        parse_mode='MarkdownV2'
    )
    logger.info(f"[{id_stamp}] User {user_id} viewed system configuration.")

async def handle_restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /restart command."""
    id_stamp = generate_id_stamp("RESTART")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # Initial logging is within restart_bot_process

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /restart command: {full_command}")

    if not is_super_admin(user_id):
        log_transaction_to_file(id_stamp, "restart", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access (not super admin)")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. เฉพาะ Super Admin เท่านั้น. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized /restart attempt by user {user_id}.")
        return

    args = context.args
    if len(args) != 1:
        log_transaction_to_file(id_stamp, "restart", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /restart (BotID) หรือ /restart all (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /restart command format from user {user_id}. Args: {args}")
        return
    
    bot_id_target = args[0].lower()
    result = await restart_bot_process(bot_id_target, user_id, username, id_stamp, full_command, {"bot_id": bot_id_target})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_backupdb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /backupdb command."""
    id_stamp = generate_id_stamp("BCKDB")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # Initial logging is within backup_database

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /backupdb command: {full_command}")

    if not is_super_admin(user_id):
        log_transaction_to_file(id_stamp, "backupdb", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access (not super admin)")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. เฉพาะ Super Admin เท่านั้น. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized /backupdb attempt by user {user_id}.")
        return
    
    await update.message.reply_text(f"⚙️ กำลังสำรองฐานข้อมูล... โปรดรอสักครู่ (Transaction ID: {id_stamp})")
    result = await backup_database(user_id, username, id_stamp, full_command, None)
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_restoredb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /restoredb command."""
    id_stamp = generate_id_stamp("RSTDB")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # Initial logging for restore is handled here, status PENDING_CONFIRMATION

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /restoredb command: {full_command}")

    if not is_super_admin(user_id):
        log_transaction_to_file(id_stamp, "restoredb", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access (not super admin)")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. เฉพาะ Super Admin เท่านั้น. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized /restoredb attempt by user {user_id}.")
        return

    args = context.args
    if len(args) != 1:
        log_transaction_to_file(id_stamp, "restoredb", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            # แก้ไข f-string ตรงนี้
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /restoredb (FileName.sql) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /restoredb command format from user {user_id}. Args: {args}")
        return
    
    backup_file_name = args[0]
    
    await update.message.reply_text(f"⚠️ คำเตือน: การกู้คืนฐานข้อมูลจะเขียนทับข้อมูลปัจจุบัน. คุณแน่ใจหรือไม่? หากแน่ใจ ให้พิมพ์ 'ยืนยัน {id_stamp}' ภายใน 30 วินาที. (Transaction ID: {id_stamp})")
    
    context.user_data[f'restore_confirm_{id_stamp}'] = {"backup_file_name": backup_file_name, "raw_command": full_command, "parsed_details": {"file_name": backup_file_name}}
    # Log the initial request, setting status to PENDING_CONFIRMATION
    log_transaction_to_file(id_stamp, "restoredb", user_id, username, full_command, {"file_name": backup_file_name}, "PENDING_CONFIRMATION", "Waiting for restore confirmation")


async def handle_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles confirmation for sensitive operations like database restore."""
    user_text = update.message.text
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    
    # Check if the text is a confirmation for a pending restore
    if user_text.lower().startswith("ยืนยัน "):
        parts = user_text.split()
        if len(parts) == 2:
            confirm_id_stamp = parts[1]
            pending_restore_data = context.user_data.get(f'restore_confirm_{confirm_id_stamp}')

            if pending_restore_data:
                # Check if this user is a super admin again
                if not is_super_admin(user_id):
                    await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ยืนยันการกระทำนี้. (Transaction ID: {confirm_id_stamp})")
                    # Log unauthorized confirmation attempt
                    log_transaction_to_file(confirm_id_stamp, "restore_confirm", user_id, username, user_text, pending_restore_data, "FAILED", "Unauthorized confirmation", "User not super admin for confirmation.")
                    return

                backup_file_name = pending_restore_data["backup_file_name"]
                raw_command = pending_restore_data["raw_command"]
                parsed_details = pending_restore_data["parsed_details"]
                
                # Remove from user_data immediately to prevent re-use
                context.user_data.pop(f'restore_confirm_{confirm_id_stamp}') 
                
                logger.info(f"[{confirm_id_stamp}] User {user_id} confirmed database restore for {backup_file_name}.")
                
                await update.message.reply_text(f"⚙️ กำลังกู้คืนฐานข้อมูลจาก `{backup_file_name}`... โปรดรอสักครู่ (Transaction ID: {confirm_id_stamp})")
                
                # Log the confirmed status and then proceed with restore
                # The actual restore_database function will update the final status
                result = await restore_database(backup_file_name, user_id, username, confirm_id_stamp, raw_command, parsed_details)
                await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {confirm_id_stamp}):\n{result}")
                return
    # If it's not a valid confirmation, just let it pass to other handlers (like unknown_command)


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles unknown commands."""
    id_stamp = generate_id_stamp("UNK")
    user_id = update.effective_user.id if update.effective_user else "N/A"
    username = update.effective_user.username if update.effective_user and update.effective_user.username else str(user_id)
    full_command = update.message.text if update.message else "N/A"
    
    log_transaction_to_file(id_stamp, "unknown", user_id, username, full_command, None, "FAILED", "Unknown command", f"Command not recognized: {full_command}")

    logger.warning(f"[{id_stamp}] User {user_id} ({username}) issued unknown command: {full_command}")
    await update.message.reply_text(
        f"ขออภัย ไม่เข้าใจคำสั่ง '{full_command}'. กรุณาลองอีกครั้ง. (Transaction ID: {id_stamp})"
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a message to the user."""
    id_stamp = generate_id_stamp("ERR")
    user_id = update.effective_user.id if update.effective_user else "N/A"
    username = update.effective_user.username if update.effective_user and update.effective_user.username else str(user_id)
    full_command = update.message.text if update.message else "N/A"
    
    error_details_str = str(context.error)
    log_transaction_to_file(id_stamp, "error", user_id, username, full_command, None, "FAILED", f"System error: {context.error}", error_details_str)

    logger.error(f"[{id_stamp}] Update {update} caused error {context.error}")
    if update.effective_message:
        await update.effective_message.reply_text(
            f"⚠️ เกิดข้อผิดพลาดภายในระบบ. โปรดลองใหม่อีกครั้ง หรือติดต่อผู้ดูแลระบบ. (Transaction ID: {id_stamp})"
        )


def main():
    """Starts the bot."""
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Register command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("adduser", handle_adduser_command))
    application.add_handler(CommandHandler("removeuser", handle_removeuser_command))
    application.add_handler(CommandHandler("viewlogs", handle_viewlogs_command))
    application.add_handler(CommandHandler("config", handle_config_command))
    application.add_handler(CommandHandler("restart", handle_restart_command))
    application.add_handler(CommandHandler("backupdb", handle_backupdb_command))
    application.add_handler(CommandHandler("restoredb", handle_restoredb_command))
    
    # Handler for general text messages, specifically for confirmation
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_confirmation))


    # Register handler for unknown commands
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    # Register error handler
    application.add_error_handler(error_handler)

    logger.info(f"Bot4 is polling...")
    application.run_polling()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot4 stopped by user.")
    except Exception as e:
        logger.critical(f"Bot4 encountered a critical error and stopped: {e}")
    finally:
        if DB_CONNECTOR:
            DB_CONNECTOR.disconnect()
            logger.info("Bot4 database disconnected.")