import logging
import os
import sys
import asyncio
from datetime import datetime, date, timedelta
import json

# Add parent directory to sys.path to allow importing from Function/ and Database/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from telegram import Update, InputFile
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler

# Import utility functions
from Function.utils import load_config, setup_logging, generate_id_stamp, is_user_allowed, parse_multi_param_command, log_transaction_to_file, update_transaction_log_file_status, TRANSACTION_LOG_FILE # <--- Import TRANSACTION_LOG_FILE
# Import database connector
from Database.db_connector import DatabaseConnector
# Import excel exporter
from Function.excel_exporter import export_to_excel

# --- Global Variables & Initialization ---
BOT_ID = "bot3"
BOT_TOKEN = None
# DB_CONNECTOR = None # Comment out or remove global DB_CONNECTOR here
INVENTORY_SETTINGS = {}
TEMP_EXCEL_DIR = os.path.join(os.path.dirname(__file__), '..', 'temp_excel_output')


# Load configs and setup logging
try:
    config = load_config(os.path.join(os.path.dirname(__file__), '..', 'Config', 'config.json'))
    tokens = load_config(os.path.join(os.path.dirname(__file__), '..', 'Config', 'token.json'))

    if not config or not tokens:
        logging.error("Failed to load config or tokens. Exiting.")
        sys.exit(1)

    BOT_TOKEN = tokens.get("BOT3_TOKEN")
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_3":
        logging.error("BOT3_TOKEN not found or is placeholder. Please update Config/token.json")
        sys.exit(1)

    # Setup logging for Bot3
    log_level = config.get("LOGGING_CONFIG", {}).get("LEVEL", "INFO").upper()
    logger = setup_logging(BOT_ID, log_level)
    logger.info(f"--- Starting Bot3: {BOT_ID} ---")

    # DB_CONNECTOR will now be instantiated inside functions for fresh connection
    # Or, if you want a global instance, ensure it's reconnected/refreshed when used.
    # For simplicity and to ensure fresh data, we will instantiate it per request if not already.

    # Initial DB connection test (optional, can be removed if connection is always on demand)
    # db_test_conn = DatabaseConnector()
    # try:
    #     db_test_conn.connect()
    #     logger.info("Bot3 successfully connected to database (initial test).")
    # except Exception as e:
    #     logger.error(f"Bot3 failed to connect to database (initial test): {e}. Some functionalities might be impaired.")
    # finally:
    #     db_test_conn.disconnect() # Disconnect after initial test

    INVENTORY_SETTINGS = config.get("INVENTORY_SETTINGS", {})

except Exception as e:
    logging.critical(f"Critical error during Bot3 initialization: {e}")
    sys.exit(1)

# --- Helper function for sending Excel file ---
async def send_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_path: str, id_stamp: str):
    """Sends an Excel file to the user and then deletes it."""
    if file_path and os.path.exists(file_path):
        try:
            with open(file_path, 'rb') as f:
                await update.message.reply_document(
                    document=InputFile(f, filename=os.path.basename(file_path)),
                    caption=f"รายงานของคุณพร้อมแล้ว (Transaction ID: {id_stamp})"
                )
            logger.info(f"[{id_stamp}] Excel file '{file_path}' sent to user {update.effective_user.id}.")
        except Exception as e:
            logger.error(f"[{id_stamp}] Error sending Excel file '{file_path}': {e}")
            await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการส่งไฟล์ Excel. (Transaction ID: {id_stamp})")
        finally:
            os.remove(file_path)
            logger.info(f"[{id_stamp}] Excel file '{file_path}' deleted from temp directory.")
    else:
        await update.message.reply_text(f"❌ ไม่สามารถสร้างไฟล์ Excel ได้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Excel file not found or not created at '{file_path}'.")

# --- Database Interaction Functions ---

# Create a module-level DB_CONNECTOR instance for this bot
# This ensures it's always initialized, and we can manage its connection lifecycle
_bot3_db_connector = DatabaseConnector()

async def get_stock_data(sku=None, include_zero_quantity=True):
    """
    Fetches stock data from the database.
    Args:
        sku (str, optional): Specific SKU to fetch. Defaults to None (fetch all).
        include_zero_quantity (bool): If True, includes items with quantity = 0.
                                      If False, only includes quantity > 0.
    """
    try:
        # Ensure connection is active and fresh
        _bot3_db_connector.connect() 
        query = """
        SELECT sku, lot, location, quantity, inbound_date
        FROM inventory
        """
        conditions = []
        params = []

        if sku:
            conditions.append("sku = %s")
            params.append(sku)
        
        if not include_zero_quantity:
            conditions.append("quantity > 0")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY sku, inbound_date ASC"
        
        return _bot3_db_connector.execute_query(query, tuple(params) if params else None, fetch_all=True)
    except Exception as e:
        logger.error(f"Error fetching stock data: {e}")
        return None # Return None on error

async def get_total_reserved_quantity(sku):
    """Fetches total reserved quantity for a given SKU."""
    try:
        _bot3_db_connector.connect() # Ensure connection is active
        query = "SELECT SUM(quantity) FROM reservations WHERE sku = %s AND status = 'PENDING'"
        result = _bot3_db_connector.execute_query(query, (sku,), fetch_one=True)
        return result[0] if result and result[0] is not None else 0
    except Exception as e:
        logger.error(f"Error fetching total reserved quantity: {e}")
        return 0

async def get_all_history_data(sku):
    """Fetches historical inventory movements for a given SKU."""
    try:
        _bot3_db_connector.connect() # Ensure connection is active
        query = """
        SELECT sku, lot, location, quantity, last_updated_date, id_stamp
        FROM inventory
        WHERE sku = %s
        ORDER BY last_updated_date DESC
        """
        return _bot3_db_connector.execute_query(query, (sku,), fetch_all=True)
    except Exception as e:
        logger.error(f"Error fetching history data: {e}")
        return None

async def get_low_stock_data():
    """Fetches items with quantity below a configured threshold."""
    try:
        _bot3_db_connector.connect() # Ensure connection is active
        threshold = INVENTORY_SETTINGS.get("LOW_STOCK_THRESHOLD", 10)
        query = """
        SELECT sku, SUM(quantity) as total_quantity
        FROM inventory
        GROUP BY sku
        HAVING total_quantity <= %s
        ORDER BY total_quantity ASC
        """
        return _bot3_db_connector.execute_query(query, (threshold,), fetch_all=True)
    except Exception as e:
        logger.error(f"Error fetching low stock data: {e}")
        return None

async def search_inventory_data(search_term):
    """Searches inventory across various fields."""
    try:
        _bot3_db_connector.connect() # Ensure connection is active
        search_pattern = f"%{search_term}%"
        query = """
        SELECT sku, lot, location, quantity, inbound_date
        FROM inventory
        WHERE sku LIKE %s OR lot LIKE %s OR location LIKE %s
        ORDER BY sku, lot
        """
        return _bot3_db_connector.execute_query(query, (search_pattern, search_pattern, search_pattern), fetch_all=True)
    except Exception as e:
        logger.error(f"Error searching inventory data: {e}")
        return None

async def get_location_data(location_name, include_zero_quantity=True):
    """Fetches all items within a specific location."""
    try:
        _bot3_db_connector.connect() # Ensure connection is active
        query = """
        SELECT sku, lot, quantity, inbound_date
        FROM inventory
        WHERE location = %s
        """
        params = [location_name]

        if not include_zero_quantity:
            query += " AND quantity > 0"
        
        query += " ORDER BY sku, inbound_date ASC"
        
        return _bot3_db_connector.execute_query(query, tuple(params), fetch_all=True)
    except Exception as e:
        logger.error(f"Error fetching location data: {e}")
        return None

async def generate_report_data(report_type, params, id_stamp):
    """Generates data for various report types."""
    data = []
    headers = []
    title = ""
    error_message = None
    
    if report_type == "stock_on_date":
        # ... (โค้ดเดิม) ...
        report_date_str = params[0] if params else None
        if not report_date_str:
            error_message = "❌ โปรดระบุวันที่สำหรับรายงาน (YYYY-MM-DD)"
        else:
            try:
                report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
                _bot3_db_connector.connect() # Ensure connection is active
                query = """
                SELECT sku, lot, location, quantity, inbound_date
                FROM inventory
                WHERE inbound_date <= %s
                ORDER BY sku, lot
                """
                data = _bot3_db_connector.execute_query(query, (report_date,), fetch_all=True)
                headers = ["SKU", "Lot", "Location", "Quantity", "Inbound Date"]
                title = f"รายงานสินค้าคงเหลือ ณ วันที่ {report_date_str}"
            except ValueError:
                error_message = "❌ รูปแบบวันที่ไม่ถูกต้อง (YYYY-MM-DD)"
            except Exception as e:
                error_message = f"❌ เกิดข้อผิดพลาดในการดึงข้อมูลสต็อก: {e}"
                logger.error(f"[{id_stamp}] Error fetching historical stock data: {e}")


    elif report_type == "movement":
        start_date_str = params[0] if len(params) > 0 else None
        end_date_str = params[1] if len(params) > 1 else None

        if not start_date_str or not end_date_str:
            error_message = "❌ โปรดระบุวันที่เริ่มต้นและสิ้นสุด (YYYY-MM-DD/YYYY-MM-DD)"
        else:
            try:
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                
                # --- START OF MODIFIED SECTION FOR MOVEMENT REPORT ---
                # Read from transactions.log file instead of querying DB table
                all_transactions = []
                if os.path.exists(TRANSACTION_LOG_FILE):
                    try:
                        with open(TRANSACTION_LOG_FILE, 'r', encoding='utf-8') as f:
                            for line in f:
                                try:
                                    entry = json.loads(line.strip())
                                    all_transactions.append(entry)
                                except json.JSONDecodeError:
                                    logger.warning(f"Skipping malformed JSON line in transactions.log: {line.strip()}")
                    except Exception as e:
                        error_message = f"❌ เกิดข้อผิดพลาดในการอ่านไฟล์ transactions.log: {e}"
                        logger.error(f"[{id_stamp}] Error reading transactions.log for movement report: {e}")
                
                # Filter transactions by date range and status
                # Also filter for relevant command_types for movement
                filtered_movements = []
                # Ensure start_datetime and end_datetime include entire day
                start_datetime = datetime.combine(start_date, datetime.min.time())
                end_datetime = datetime.combine(end_date, datetime.max.time())

                for entry in all_transactions:
                    # Only consider initial log entries (not status updates)
                    if entry.get("command_type") and entry.get("status") == "SUCCESS": # Only successful transactions
                        try: # Added try-except for datetime parsing
                            log_timestamp = datetime.fromisoformat(entry["timestamp"])
                        except ValueError:
                            logger.warning(f"Skipping entry with invalid timestamp: {entry.get('timestamp')}")
                            continue

                        if start_datetime <= log_timestamp <= end_datetime:
                            # Filter for commands that represent item movements
                            relevant_commands = ['in', 'out', 'adjust_in', 'return', 'reserve', 'reserve_pick', 'reserve_return', 'reserve_cancel']
                            if entry["command_type"] in relevant_commands:
                                # Extract relevant details from parsed_details or raw_command
                                extracted_sku = None
                                extracted_qty = None
                                
                                # This part needs careful handling depending on parsed_details structure for each command
                                # Example for how to extract for /in, /out, /adjust_in, /return (multi-item)
                                if entry.get("parsed_details"):
                                    parsed_detail_items = entry["parsed_details"].get("args", [])
                                    # Assuming parsed_detail_items is a list of lists: [['SKU1', 'QTY1', ...], ['SKU2', 'QTY2', ...]]
                                    if parsed_detail_items and isinstance(parsed_detail_items, list) and len(parsed_detail_items) > 0 and isinstance(parsed_detail_items[0], list):
                                        # For simplicity, we aggregate details if multiple items.
                                        # For more detailed report, you might flatten this loop here or process each sub-item separately.
                                        skus_in_cmd = [item[0] for item in parsed_detail_items]
                                        quantities_in_cmd = []
                                        for item in parsed_detail_items:
                                            try:
                                                quantities_in_cmd.append(int(item[1]))
                                            except (ValueError, IndexError):
                                                pass # Ignore if quantity is malformed
                                        
                                        extracted_sku = ", ".join(skus_in_cmd) # e.g., "SKU001, SKU002"
                                        extracted_qty = sum(quantities_in_cmd) # Sum quantities for multi-item commands
                                    elif entry["command_type"] in ['reserve', 'cancel_out', 'reserve_pick', 'reserve_return', 'reserve_cancel'] and entry["parsed_details"].get("args"):
                                        # For single item commands, arguments might be a flat list of strings
                                        args_list = entry["parsed_details"]["args"]
                                        if len(args_list) >= 2: # At least SKU and Quantity
                                            extracted_sku = args_list[0]
                                            try:
                                                extracted_qty = int(args_list[1])
                                            except ValueError:
                                                extracted_qty = None
                                        elif entry["command_type"] == 'reserve_cancel' and entry["parsed_details"].get("file_name"): # For restore confirmation
                                            extracted_sku = entry["parsed_details"].get("reserve_id", "N/A") # This is for reserve_cancel, not typical movement
                                            extracted_qty = None # Quantity needs lookup from DB/other logs for cancellation

                                # Determine direction for display
                                movement_direction = "N/A"
                                if entry["command_type"] == 'in': movement_direction = "รับเข้า (+)"
                                elif entry["command_type"] == 'return': movement_direction = "คืนเข้า (+)"
                                elif entry["command_type"] == 'out': movement_direction = "ส่งออก (-)"
                                elif entry["command_type"] == 'adjust_in' and extracted_qty is not None:
                                    if extracted_qty > 0: movement_direction = "ปรับเพิ่ม (+)"
                                    elif extracted_qty < 0: movement_direction = "ปรับลด (-)"
                                elif entry["command_type"] == 'reserve': movement_direction = "จอง (↓)"
                                elif entry["command_type"] == 'reserve_pick': movement_direction = "หยิบจอง (↓)"
                                elif entry["command_type"] == 'reserve_return': movement_direction = "คืนจอง (↑)"
                                elif entry["command_type"] == 'reserve_cancel': movement_direction = "ยกเลิกจอง (↑)"
                                
                                # Append data for the report
                                filtered_movements.append({
                                    "id_stamp": entry.get("id_stamp", "N/A"),
                                    "Command Type": entry.get("command_type", "N/A"),
                                    "Movement Type": movement_direction,
                                    "SKU": extracted_sku,
                                    "Quantity": extracted_qty,
                                    "Timestamp": entry.get("timestamp", "N/A"),
                                    "User ID": entry.get("user_id", "N/A"),
                                    "Username": entry.get("username", "N/A"),
                                    "Message": entry.get("message", "N/A"),
                                    "Raw Command": entry.get("raw_command", "N/A")
                                })
                
                data = [[item[header] for header in ["id_stamp", "Command Type", "Movement Type", "SKU", "Quantity", "Timestamp", "User ID", "Username", "Message", "Raw Command"]] for item in filtered_movements]
                headers = ["Transaction ID", "Type", "Movement", "SKU", "Quantity", "Timestamp", "User ID", "Username", "Message", "Raw Command"]
                title = f"รายงานการเคลื่อนไหวสินค้า ({start_date_str} ถึง {end_date_str})"
            except ValueError:
                error_message = "❌ รูปแบบวันที่ไม่ถูกต้อง (YYYY-MM-DD)"
            except Exception as e:
                error_message = f"❌ เกิดข้อผิดพลาดในการประมวลผลไฟล์ Log สำหรับรายงานการเคลื่อนไหว: {e}"
                logger.error(f"[{id_stamp}] Error processing transactions.log for movement report: {e}")

        # --- END OF MODIFIED SECTION FOR MOVEMENT REPORT ---

    elif report_type == "low_stock_alert":
        data = await get_low_stock_data() # Reuse existing function
        headers = ["SKU", "Total Quantity"]
        title = "รายงานสินค้าใกล้หมด"

    elif report_type == "by_location":
        target_location = params[0] if params else None
        if target_location:
            data = await get_location_data(target_location)
            headers = ["SKU", "Lot", "Quantity", "Inbound Date"]
            title = f"รายงานสต็อกใน Location: {target_location}"
        else:
            # Report for all locations, group by location
            try:
                _bot3_db_connector.connect() # Ensure connection is active
                query = """
                SELECT location, sku, lot, quantity, inbound_date
                FROM inventory
                -- WHERE quantity > 0 -- No quantity > 0 filter for comprehensive location report
                ORDER BY location, sku, inbound_date
                """
                data = _bot3_db_connector.execute_query(query, fetch_all=True)
                headers = ["Location", "SKU", "Lot", "Quantity", "Inbound Date"]
                title = "รายงานสต็อกแยกตาม Location ทั้งหมด"
            except Exception as e:
                error_message = f"❌ เกิดข้อผิดพลาดในการดึงข้อมูลสต็อกแยกตาม Location: {e}"
                logger.error(f"[{id_stamp}] Error fetching by_location data: {e}")
    else:
        error_message = "❌ ไม่รู้จักประเภทรายงาน. ประเภทที่รองรับ: stock_on_date, movement, low_stock_alert, by_location"
    
    if error_message:
        return None, error_message, None
    
    return data, title, headers

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
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", "Welcome message sent")
    await update.message.reply_text(
        f"สวัสดีครับ! ยินดีต้อนรับสู่ Bot3 (ระบบ Inventory).\n"
        f"คุณสามารถใช้คำสั่งต่อไปนี้:\n"
        f"/stock (SKU) [Excelfile]\n"
        f"/allstock [Excelfile]\n"
        f"/history (SKU)\n"
        f"/lowstock [Excelfile]\n"
        f"/search (คำค้นหา) [Excelfile]\n"
        f"/report (ประเภทรายงาน) [พารามิเตอร์เพิ่มเติม] [Excelfile]\n"
        f"/checklocation (SKU)\n"
        f"/location (Location)\n"
        f"Transaction ID: {id_stamp}"
    )

async def handle_stock_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /stock command."""
    id_stamp = generate_id_stamp("STOCK")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    log_transaction_to_file(id_stamp, "stock", user_id, username, full_command, {"args": command_args}, "PROCESSING", "Command received")

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /stock command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /stock by user {user_id}.")
        return

    args = context.args
    if not args:
        update_transaction_log_file_status(id_stamp, "FAILED", "Missing SKU parameter")
        await update.message.reply_text(f"❌ โปรดระบุ SKU. ตัวอย่าง: /stock SKU001 (Transaction ID: {id_stamp})")
        return

    sku = args[0]
    export_to_excel_flag = "excelfile" in [arg.lower() for arg in args]

    stock_data = await get_stock_data(sku, include_zero_quantity=True)
    total_reserved = await get_total_reserved_quantity(sku)

    if not stock_data:
        update_transaction_log_file_status(id_stamp, "SUCCESS", f"No stock found for SKU: {sku}")
        await update.message.reply_text(f"ℹ️ ไม่พบข้อมูลคงเหลือสำหรับ SKU: **{sku}**. (Transaction ID: {id_stamp})")
        logger.info(f"[{id_stamp}] No stock data found for SKU: {sku}.")
        return

    if export_to_excel_flag:
        headers = ["SKU", "Lot", "Location", "Quantity", "Inbound Date"]
        data_for_export = [list(row) for row in stock_data] 
        excel_file_path = export_to_excel(data_for_export, headers, f"stock_{sku}", TEMP_EXCEL_DIR)
        await send_excel_file(update, context, excel_file_path, id_stamp)
        final_message = f"ส่งออกรายงานสต็อก SKU {sku} เป็น Excel"
    else:
        response_lines = [f"ข้อมูลคงเหลือสำหรับ SKU: **{sku}**"]
        total_quantity = sum(item[3] for item in stock_data)
        response_lines.append(f"ยอดรวมคงเหลือ: {total_quantity} ชิ้น")
        if total_reserved > 0:
            response_lines.append(f"กำลังถูกจอง: {total_reserved} ชิ้น (ดูรายละเอียดเพิ่มเติมที่ /reserve_CK {sku})")

        response_lines.append("\nรายละเอียด:")
        for item in stock_data:
            inbound_date_str = item[4].strftime('%Y-%m-%d') if item[4] else "ไม่มีข้อมูลวันที่"
            response_lines.append(f"- Lot: {item[1]}, Loc: {item[2]}, จำนวน: {item[3]} ชิ้น, รับเข้า: {inbound_date_str}")
        
        await update.message.reply_text(
            f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n" + "\n".join(response_lines),
            parse_mode='Markdown'
        )
        final_message = f"แสดงรายงานสต็อก SKU {sku}"
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", final_message)
    logger.info(f"[{id_stamp}] User {user_id} checked stock for SKU: {sku}.")

async def handle_allstock_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /allstock command."""
    id_stamp = generate_id_stamp("ALLSTOCK")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    log_transaction_to_file(id_stamp, "allstock", user_id, username, full_command, {"args": command_args}, "PROCESSING", "Command received")

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /allstock command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /allstock by user {user_id}.")
        return

    export_to_excel_flag = "excelfile" in [arg.lower() for arg in context.args]

    all_stock_data = await get_stock_data(include_zero_quantity=True) # <--- แก้ไข: เรียกใช้ get_stock_data ด้วย include_zero_quantity=True

    if not all_stock_data:
        update_transaction_log_file_status(id_stamp, "SUCCESS", "No stock data found")
        await update.message.reply_text(f"ℹ️ ไม่พบข้อมูลสินค้าคงเหลือในคลัง. (Transaction ID: {id_stamp})")
        logger.info(f"[{id_stamp}] No stock data found for /allstock.")
        return

    if export_to_excel_flag:
        headers = ["SKU", "Lot", "Location", "Quantity", "Inbound Date"]
        data_for_export = [list(row) for row in all_stock_data]
        excel_file_path = export_to_excel(data_for_export, headers, "all_stock_report", TEMP_EXCEL_DIR)
        await send_excel_file(update, context, excel_file_path, id_stamp)
        final_message = "ส่งออกรายงานสต็อกทั้งหมดเป็น Excel"
    else:
        response_lines = ["รายการสินค้าคงเหลือทั้งหมด:"]
        # แสดงรายการที่ quantity เป็น 0 ด้วย
        current_stock_lines = []
        zero_stock_lines = []

        for item in all_stock_data:
            inbound_date_str = item[4].strftime('%Y-%m-%d') if item[4] else "ไม่มีข้อมูลวันที่"
            line = f"- **{item[0]}** (Lot: {item[1]}, Loc: {item[2]}): {item[3]} ชิ้น (รับเข้า: {inbound_date_str})"
            if item[3] > 0: # Quantity
                current_stock_lines.append(line)
            else:
                zero_stock_lines.append(line)
        
        if current_stock_lines:
            response_lines.append("\n**สินค้าคงเหลือในคลัง:**")
            response_lines.extend(current_stock_lines)
        
        if zero_stock_lines:
            response_lines.append("\n**สินค้าที่จำนวนเป็น 0 (หมดสต็อก):**")
            response_lines.extend(zero_stock_lines)

        if not current_stock_lines and not zero_stock_lines:
            response_lines.append("ไม่มีสินค้าในคลัง.")


        await update.message.reply_text(
            f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n" + "\n".join(response_lines),
            parse_mode='Markdown'
        )
        final_message = "แสดงรายงานสต็อกทั้งหมด"
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", final_message)
    logger.info(f"[{id_stamp}] User {user_id} checked all stock.")

async def handle_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /history command."""
    id_stamp = generate_id_stamp("HIST")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    log_transaction_to_file(id_stamp, "history", user_id, username, full_command, {"args": command_args}, "PROCESSING", "Command received")

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /history command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /history by user {user_id}.")
        return

    args = context.args
    if not args:
        update_transaction_log_file_status(id_stamp, "FAILED", "Missing SKU parameter")
        await update.message.reply_text(f"❌ โปรดระบุ SKU. ตัวอย่าง: /history SKU001 (Transaction ID: {id_stamp})")
        return
    
    sku = args[0]
    history_data = await get_all_history_data(sku)

    if not history_data:
        update_transaction_log_file_status(id_stamp, "SUCCESS", f"No history found for SKU: {sku}")
        await update.message.reply_text(f"ℹ️ ไม่พบประวัติการเคลื่อนไหวสำหรับ SKU: **{sku}**. (Transaction ID: {id_stamp})")
        logger.info(f"[{id_stamp}] No history data found for SKU: {sku}.")
        return
    
    response_lines = [f"ประวัติการเคลื่อนไหวสำหรับ SKU: **{sku}**:"]
    for item in history_data:
        updated_date_str = item[4].strftime('%Y-%m-%d %H:%M') if item[4] else "ไม่มีข้อมูลวันที่"
        response_lines.append(
            f"- Lot: {item[1]}, Loc: {item[2]}, จำนวน: {item[3]} (ปรับปรุงเมื่อ: {updated_date_str}), ID: `{item[5]}`"
        )
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", f"Displayed history for SKU: {sku}")
    await update.message.reply_text(
        f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n" + "\n".join(response_lines),
        parse_mode='Markdown'
    )
    logger.info(f"[{id_stamp}] User {user_id} checked history for SKU: {sku}.")

async def handle_lowstock_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /lowstock command."""
    id_stamp = generate_id_stamp("LOWSTOCK")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    log_transaction_to_file(id_stamp, "lowstock", user_id, username, full_command, {"args": command_args}, "PROCESSING", "Command received")

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /lowstock command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /lowstock by user {user_id}.")
        return

    export_to_excel_flag = "excelfile" in [arg.lower() for arg in context.args]

    low_stock_data = await get_low_stock_data()

    if not low_stock_data:
        update_transaction_log_file_status(id_stamp, "SUCCESS", "No low stock items found")
        await update.message.reply_text(f"✅ ไม่พบสินค้าที่ใกล้หมด. (Transaction ID: {id_stamp})")
        logger.info(f"[{id_stamp}] No low stock items found.")
        return

    if export_to_excel_flag:
        headers = ["SKU", "Total Quantity"]
        data_for_export = [list(row) for row in low_stock_data]
        excel_file_path = export_to_excel(data_for_export, headers, "low_stock_report", TEMP_EXCEL_DIR)
        await send_excel_file(update, context, excel_file_path, id_stamp)
        final_message = "ส่งออกรายงานสินค้าใกล้หมดเป็น Excel"
    else:
        response_lines = [f"รายการสินค้าที่ใกล้หมด (ต่ำกว่า {INVENTORY_SETTINGS.get('LOW_STOCK_THRESHOLD', 10)} ชิ้น):"]
        for item in low_stock_data:
            response_lines.append(f"- **{item[0]}**: คงเหลือ {item[1]} ชิ้น")
        
        await update.message.reply_text(
            f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n" + "\n".join(response_lines),
            parse_mode='Markdown'
        )
        final_message = "แสดงรายงานสินค้าใกล้หมด"
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", final_message)
    logger.info(f"[{id_stamp}] User {user_id} checked low stock.")

async def handle_search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /search command."""
    id_stamp = generate_id_stamp("SEARCH")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    log_transaction_to_file(id_stamp, "search", user_id, username, full_command, {"args": command_args}, "PROCESSING", "Command received")

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /search command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /search by user {user_id}.")
        return

    args = context.args
    if not args:
        update_transaction_log_file_status(id_stamp, "FAILED", "Missing search term")
        await update.message.reply_text(f"❌ โปรดระบุคำค้นหา. ตัวอย่าง: /search กาแฟ (Transaction ID: {id_stamp})")
        return
    
    search_term_parts = []
    export_to_excel_flag = False
    for arg in args:
        if arg.lower() == "excelfile":
            export_to_excel_flag = True
        else:
            search_term_parts.append(arg)
    
    if not search_term_parts:
        update_transaction_log_file_status(id_stamp, "FAILED", "Missing search term after flag check")
        await update.message.reply_text(f"❌ โปรดระบุคำค้นหา. (Transaction ID: {id_stamp})")
        return

    search_term = " ".join(search_term_parts)

    search_results = await search_inventory_data(search_term)

    if not search_results:
        update_transaction_log_file_status(id_stamp, "SUCCESS", f"No results for search term: {search_term}")
        await update.message.reply_text(f"ℹ️ ไม่พบสินค้าที่ตรงกับคำค้นหา: '{search_term}'. (Transaction ID: {id_stamp})")
        logger.info(f"[{id_stamp}] No search results for: '{search_term}'.")
        return

    if export_to_excel_flag:
        headers = ["SKU", "Lot", "Location", "Quantity", "Inbound Date"]
        data_for_export = [list(row) for row in search_results]
        excel_file_path = export_to_excel(data_for_export, headers, f"search_results_{search_term.replace(' ', '_')}", TEMP_EXCEL_DIR)
        await send_excel_file(update, context, excel_file_path, id_stamp)
        final_message = f"ส่งออกผลการค้นหา '{search_term}' เป็น Excel"
    else:
        response_lines = [f"ผลการค้นหาสำหรับ '{search_term}':"]
        for item in search_results:
            inbound_date_str = item[4].strftime('%Y-%m-%d') if item[4] else "ไม่มีข้อมูลวันที่"
            response_lines.append(f"- **{item[0]}** (Lot: {item[1]}, Loc: {item[2]}): {item[3]} ชิ้น (รับเข้า: {inbound_date_str})")
        
        await update.message.reply_text(
            f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n" + "\n".join(response_lines),
            parse_mode='Markdown'
        )
        final_message = f"แสดงผลการค้นหา '{search_term}'"
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", final_message)
    logger.info(f"[{id_stamp}] User {user_id} searched for: '{search_term}'. Found {len(search_results)} results.")

async def handle_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /report command."""
    id_stamp = generate_id_stamp("REPORT")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    log_transaction_to_file(id_stamp, "report", user_id, username, full_command, {"args": command_args}, "PROCESSING", "Command received")

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /report command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /report by user {user_id}.")
        return

    args = context.args
    if not args:
        update_transaction_log_file_status(id_stamp, "FAILED", "Missing report type")
        await update.message.reply_text(
            f"❌ โปรดระบุประเภทรายงาน. ตัวอย่าง: /report stock_on_date (YYYY-MM-DD) [Excelfile] (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /report command format from user {user_id}. Missing report type.")
        return
    
    report_type = args[0].lower()
    report_params = []
    export_to_excel_flag = False

    for i in range(1, len(args)):
        if args[i].lower() == "excelfile":
            export_to_excel_flag = True
        else:
            report_params.append(args[i])

    await update.message.reply_text(f"⚙️ กำลังประมวลผลรายงาน '{report_type}'... โปรดรอสักครู่ (Transaction ID: {id_stamp})")

    data, title, headers = await generate_report_data(report_type, report_params, id_stamp)

    if data is None and title.startswith("❌"):
        update_transaction_log_file_status(id_stamp, "FAILED", f"Report generation error: {title}", error_details=title)
        await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{title}")
        logger.warning(f"[{id_stamp}] Error generating report '{report_type}': {title}")
        return

    if not data:
        update_transaction_log_file_status(id_stamp, "SUCCESS", f"No data found for report: {report_type}")
        await update.message.reply_text(f"ℹ️ ไม่พบข้อมูลสำหรับรายงาน: '{title}'. (Transaction ID: {id_stamp})")
        logger.info(f"[{id_stamp}] No data found for report: '{report_type}'.")
        return

    if export_to_excel_flag:
        excel_file_path = export_to_excel([list(row) for row in data], headers, title.replace(" ", "_").replace(":", ""), TEMP_EXCEL_DIR)
        await send_excel_file(update, context, excel_file_path, id_stamp)
        final_message = f"ส่งออกรายงาน '{report_type}' เป็น Excel"
    else:
        response_lines = [f"{title}:"]
        max_lines = 20
        for i, item in enumerate(data):
            if i >= max_lines:
                response_lines.append(f"...\n(แสดงเพียง {max_lines} รายการแรก. หากต้องการทั้งหมดใช้ 'Excelfile' ต่อท้ายคำสั่ง)")
                break
            response_lines.append(f"- {', '.join(map(str, item))}")
        
        await update.message.reply_text(
            f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n" + "\n".join(response_lines)
        )
        final_message = f"แสดงรายงาน '{report_type}'"
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", final_message)
    logger.info(f"[{id_stamp}] User {user_id} generated report '{report_type}'.")


async def handle_checklocation_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /checklocation command."""
    id_stamp = generate_id_stamp("CKLOC")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    log_transaction_to_file(id_stamp, "checklocation", user_id, username, full_command, {"args": command_args}, "PROCESSING", "Command received")

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /checklocation command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /checklocation by user {user_id}.")
        return

    args = context.args
    if not args:
        update_transaction_log_file_status(id_stamp, "FAILED", "Missing SKU parameter")
        await update.message.reply_text(f"❌ โปรดระบุ SKU. ตัวอย่าง: /checklocation SKU001 (Transaction ID: {id_stamp})")
        return
    
    sku = args[0]
    query = "SELECT DISTINCT location FROM inventory WHERE sku = %s" # <--- แก้ไข: ลบ quantity > 0 ออก
    locations = DB_CONNECTOR.execute_query(query, (sku,), fetch_all=True)

    if not locations:
        update_transaction_log_file_status(id_stamp, "SUCCESS", f"No locations found for SKU: {sku}")
        await update.message.reply_text(f"ℹ️ ไม่พบ SKU: **{sku}** ใน Location ใดๆ. (Transaction ID: {id_stamp})")
        logger.info(f"[{id_stamp}] No locations found for SKU: {sku}.")
        return
    
    response_lines = [f"SKU: **{sku}** พบได้ที่ Location(s):"]
    for loc_tuple in locations:
        response_lines.append(f"- {loc_tuple[0]}")
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", f"Displayed locations for SKU: {sku}")
    await update.message.reply_text(
        f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n" + "\n".join(response_lines),
        parse_mode='Markdown'
    )
    logger.info(f"[{id_stamp}] User {user_id} checked location for SKU: {sku}.")


async def handle_location_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /location command."""
    id_stamp = generate_id_stamp("LOC")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    log_transaction_to_file(id_stamp, "location", user_id, username, full_command, {"args": command_args}, "PROCESSING", "Command received")

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /location command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        update_transaction_log_file_status(id_stamp, "FAILED", "Unauthorized access")
        await update.message.reply_text(f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})")
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /location by user {user_id}.")
        return

    args = context.args
    if not args:
        update_transaction_log_file_status(id_stamp, "FAILED", "Missing location parameter")
        await update.message.reply_text(f"❌ โปรดระบุ Location. ตัวอย่าง: /location A101 (Transaction ID: {id_stamp})")
        return
    
    location_name = " ".join(args)

    location_data = await get_location_data(location_name, include_zero_quantity=True) # <--- แก้ไข: เรียกใช้ get_location_data ด้วย include_zero_quantity=True

    if not location_data:
        update_transaction_log_file_status(id_stamp, "SUCCESS", f"No items found in location: {location_name}")
        await update.message.reply_text(f"ℹ️ ไม่พบสินค้าใน Location: **{location_name}**. (Transaction ID: {id_stamp})")
        logger.info(f"[{id_stamp}] No items found in location: {location_name}.")
        return
    
    response_lines = [f"รายการสินค้าใน Location: **{location_name}**:"]
    # แสดงรายการที่ quantity เป็น 0 ด้วย
    current_loc_stock_lines = []
    zero_loc_stock_lines = []

    for item in location_data:
        inbound_date_str = item[3].strftime('%Y-%m-%d') if item[3] else "ไม่มีข้อมูลวันที่"
        line = f"- **{item[0]}** (Lot: {item[1]}): จำนวน {item[2]} ชิ้น (รับเข้า: {inbound_date_str})"
        if item[2] > 0: # Quantity
            current_loc_stock_lines.append(line)
        else:
            zero_loc_stock_lines.append(line)
    
    if current_loc_stock_lines:
        response_lines.append("\n**สินค้าใน Location (คงเหลือ):**")
        response_lines.extend(current_loc_stock_lines)
    
    if zero_loc_stock_lines:
        response_lines.append("\n**สินค้าใน Location (หมดสต็อก):**")
        response_lines.extend(zero_loc_stock_lines)

    if not current_loc_stock_lines and not zero_loc_stock_lines:
        response_lines.append("ไม่มีสินค้าใน Location นี้เลย.")

    await update.message.reply_text(
        f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n" + "\n".join(response_lines),
        parse_mode='Markdown'
    )
    logger.info(f"[{id_stamp}] User {user_id} checked items in location: {location_name}.")


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
    application.add_handler(CommandHandler("stock", handle_stock_command))
    application.add_handler(CommandHandler("allstock", handle_allstock_command))
    application.add_handler(CommandHandler("history", handle_history_command))
    application.add_handler(CommandHandler("lowstock", handle_lowstock_command))
    application.add_handler(CommandHandler("search", handle_search_command))
    application.add_handler(CommandHandler("report", handle_report_command))
    application.add_handler(CommandHandler("checklocation", handle_checklocation_command))
    application.add_handler(CommandHandler("location", handle_location_command))


    # Register handler for unknown commands
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    # Register error handler
    application.add_error_handler(error_handler)

    logger.info(f"Bot3 is polling...")
    application.run_polling()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot3 stopped by user.")
    except Exception as e:
        logger.critical(f"Bot3 encountered a critical error and stopped: {e}")
    finally:
        # DB_CONNECTOR is now a module-level variable used directly in functions
        # Disconnect it only once if it's the last use, or manage its lifecycle
        # as a singleton if multiple parts need it. For now, we connect per function.
        # So, no global disconnect needed here for Bot3 if _bot3_db_connector handles its own.
        # If _bot3_db_connector is a global instance, it should be disconnected here.
        # Let's ensure _bot3_db_connector is disconnected when the bot stops.
        _bot3_db_connector.disconnect() # <--- แก้ไข: เพิ่มการ Disconnect Global DB Connector ของ Bot3
        logger.info("Bot3 database disconnected.")