import logging
import os
import sys
import asyncio
from datetime import datetime
import json # Import json for parsed_details

# Add parent directory to sys.path to allow importing from Function/ and Database/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler

# Import utility functions
# No need for set_db_connector if transaction logs are only in files.
from Function.utils import load_config, setup_logging, generate_id_stamp, is_user_allowed, parse_multi_param_command, log_transaction_to_file, update_transaction_log_file_status
# Import database connector (still needed for inventory and reservations)
from Database.db_connector import DatabaseConnector

# --- Global Variables & Initialization ---
BOT_ID = "bot1"
BOT_TOKEN = None
DB_CONNECTOR = None
INVENTORY_SETTINGS = {}

# Load configs and setup logging
try:
    config = load_config(os.path.join(os.path.dirname(__file__), '..', 'Config', 'config.json'))
    tokens = load_config(os.path.join(os.path.dirname(__file__), '..', 'Config', 'token.json'))

    if not config or not tokens:
        logging.error("Failed to load config or tokens. Exiting.")
        sys.exit(1)

    BOT_TOKEN = tokens.get("BOT1_TOKEN")
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_1":
        logging.error("BOT1_TOKEN not found or is placeholder. Please update Config/token.json")
        sys.exit(1)

    # Setup logging for Bot1
    log_level = config.get("LOGGING_CONFIG", {}).get("LEVEL", "INFO").upper()
    logger = setup_logging(BOT_ID, log_level)
    logger.info(f"--- Starting Bot1: {BOT_ID} ---")

    # Initialize Database Connector (still needed for inventory and reservations tables)
    DB_CONNECTOR = DatabaseConnector()
    # No need to set_db_connector for utils if transaction logs are to file.

    try:
        DB_CONNECTOR.connect()
        logger.info("Bot1 successfully connected to database.")
    except Exception as e:
        logger.error(f"Bot1 failed to connect to database: {e}. Some functionalities might be impaired.")

    INVENTORY_SETTINGS = config.get("INVENTORY_SETTINGS", {})

except Exception as e:
    logging.critical(f"Critical error during Bot1 initialization: {e}")
    sys.exit(1)

# --- Database Interaction Functions ---

async def process_inbound_item(skus, quantities, lots, dates, locations, user_id, username, id_stamp, raw_command, parsed_details):
    """Processes a single inbound item or multiple items from /in command."""
    results = []
    overall_success = True
    error_message_summary = []

    # Log initial state for the whole transaction
    log_transaction_to_file(id_stamp, "in", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing inbound items")

    try:
        for sku, qty_str, lot, date_str, loc in zip(skus, quantities, lots, dates, locations):
            item_success = True # Flag for individual item success
            item_error = None
            try:
                quantity = int(qty_str)
                if quantity <= 0:
                    results.append(f"❌ SKU: {sku} - จำนวนต้องเป็นบวก")
                    item_success = False
                    item_error = "Invalid quantity (non-positive)"
                    continue
                
                try:
                    record_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    results.append(f"❌ SKU: {sku} - รูปแบบวันที่ไม่ถูกต้อง (YYYY-MM-DD)")
                    item_success = False
                    item_error = "Invalid date format"
                    continue

                check_query = "SELECT quantity FROM inventory WHERE sku = %s AND lot = %s AND location = %s"
                existing_record = DB_CONNECTOR.execute_query(check_query, (sku, lot, loc), fetch_one=True)

                if existing_record:
                    new_quantity = existing_record[0] + quantity
                    update_query = """
                    UPDATE inventory SET quantity = %s, last_updated_date = %s, last_updated_by = %s, id_stamp = %s
                    WHERE sku = %s AND lot = %s AND location = %s
                    """
                    DB_CONNECTOR.execute_query(update_query, 
                                               (new_quantity, datetime.now(), user_id, id_stamp, sku, lot, loc), 
                                               commit=True)
                    results.append(f"✅ SKU: {sku}, จำนวน: {quantity}, Lot: {lot}, Loc: {loc} - เพิ่มจำนวนเป็น {new_quantity} แล้ว")
                    logger.info(f"[{id_stamp}] Updated inventory for SKU {sku}, Lot {lot}, Loc {loc} to {new_quantity}. By User: {user_id}")
                else:
                    insert_query = """
                    INSERT INTO inventory (sku, lot, location, quantity, inbound_date, created_by, id_stamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    DB_CONNECTOR.execute_query(insert_query, 
                                               (sku, lot, loc, quantity, record_date, user_id, id_stamp), 
                                               commit=True)
                    results.append(f"✅ SKU: {sku}, จำนวน: {quantity}, Lot: {lot}, Loc: {loc} - บันทึกรับเข้าใหม่แล้ว")
                    logger.info(f"[{id_stamp}] Recorded new inbound for SKU {sku}, Lot {lot}, Loc {loc} with quantity {quantity}. By User: {user_id}")

            except ValueError as ve:
                results.append(f"❌ SKU: {sku} - จำนวนหรือรูปแบบไม่ถูกต้อง: {ve}")
                logger.warning(f"[{id_stamp}] Invalid data for SKU {sku}. Error: {ve}. User: {user_id}.")
                item_success = False
                item_error = f"ValueError: {ve}"
            except Exception as e:
                results.append(f"❌ SKU: {sku} - เกิดข้อผิดพลาด: {e}")
                logger.error(f"[{id_stamp}] Error processing /in for SKU {sku}. Error: {e}")
                item_success = False
                item_error = f"General error: {e}"
            
            if not item_success:
                overall_success = False
                error_message_summary.append(f"SKU {sku} (Lot: {lot}, Loc: {loc}): {item_error}")

    except Exception as e:
        logger.error(f"[{id_stamp}] General error in process_inbound_item: {e}")
        results.append(f"❌ เกิดข้อผิดพลาดทั่วไปในการประมวลผล: {e}")
        overall_success = False
        error_message_summary.append(f"Overall processing error: {e}")
    
    # Update transaction log file with final status
    final_status = "SUCCESS" if overall_success and not error_message_summary else "FAILED"
    final_message = "รับสินค้าสำเร็จทุกรายการ" if overall_success and not error_message_summary else "รับสินค้าล้มเหลวบางรายการ"
    error_details = ", ".join(error_message_summary) if error_message_summary else None
    
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details) 

    return results

async def process_return_item(skus, quantities, lots, dates, locations, user_id, username, id_stamp, raw_command, parsed_details):
    """Processes a single return item or multiple items from /return command."""
    results = []
    overall_success = True
    error_message_summary = []

    log_transaction_to_file(id_stamp, "return", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing return items")

    try:
        for sku, qty_str, lot, date_str, loc in zip(skus, quantities, lots, dates, locations):
            item_success = True
            item_error = None
            try:
                quantity = int(qty_str)
                if quantity <= 0:
                    results.append(f"❌ SKU: {sku} - จำนวนต้องเป็นบวก")
                    item_success = False
                    item_error = "Invalid quantity (non-positive)"
                    continue
                
                try:
                    return_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    results.append(f"❌ SKU: {sku} - รูปแบบวันที่ไม่ถูกต้อง (YYYY-MM-DD)")
                    item_success = False
                    item_error = "Invalid date format"
                    continue

                check_query = "SELECT quantity FROM inventory WHERE sku = %s AND lot = %s AND location = %s"
                existing_record = DB_CONNECTOR.execute_query(check_query, (sku, lot, loc), fetch_one=True)

                if existing_record:
                    new_quantity = existing_record[0] + quantity
                    update_query = """
                    UPDATE inventory SET quantity = %s, last_updated_date = %s, last_updated_by = %s, id_stamp = %s
                    WHERE sku = %s AND lot = %s AND location = %s
                    """
                    DB_CONNECTOR.execute_query(update_query, 
                                               (new_quantity, datetime.now(), user_id, id_stamp, sku, lot, loc), 
                                               commit=True)
                    results.append(f"✅ SKU: {sku}, จำนวน: {quantity}, Lot: {lot}, Loc: {loc} - คืนสินค้าเข้าระบบแล้ว")
                    logger.info(f"[{id_stamp}] Returned item for SKU {sku}, Lot {lot}, Loc {loc}, new quantity {new_quantity}. By User: {user_id}")
                else:
                    insert_query = """
                    INSERT INTO inventory (sku, lot, location, quantity, inbound_date, created_by, id_stamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    DB_CONNECTOR.execute_query(insert_query, 
                                               (sku, lot, loc, quantity, return_date, user_id, id_stamp), 
                                               commit=True)
                    results.append(f"✅ SKU: {sku}, จำนวน: {quantity}, Lot: {lot}, Loc: {loc} - บันทึกคืนสินค้าใหม่แล้ว")
                    logger.info(f"[{id_stamp}] Recorded new return/inbound for SKU {sku}, Lot {lot}, Loc {loc} with quantity {quantity}. By User: {user_id}")

            except ValueError as ve:
                results.append(f"❌ SKU: {sku} - จำนวนหรือรูปแบบไม่ถูกต้อง: {ve}")
                logger.warning(f"[{id_stamp}] Invalid data for return SKU {sku}. Error: {ve}. User: {user_id}.")
                item_success = False
                item_error = f"ValueError: {ve}"
            except Exception as e:
                results.append(f"❌ SKU: {sku} - เกิดข้อผิดพลาด: {e}")
                logger.error(f"[{id_stamp}] Error processing /return for SKU {sku}. Error: {e}")
                item_success = False
                item_error = f"General error: {e}"
            
            if not item_success:
                overall_success = False
                error_message_summary.append(f"SKU {sku} (Lot: {lot}, Loc: {loc}): {item_error}")
    except Exception as e:
        logger.error(f"[{id_stamp}] General error in process_return_item: {e}")
        results.append(f"❌ เกิดข้อผิดพลาดทั่วไปในการประมวลผล: {e}")
        overall_success = False
        error_message_summary.append(f"Overall processing error: {e}")
    
    final_status = "SUCCESS" if overall_success and not error_message_summary else "FAILED"
    final_message = "คืนสินค้าสำเร็จทุกรายการ" if overall_success and not error_message_summary else "คืนสินค้าล้มเหลวบางรายการ"
    error_details = ", ".join(error_message_summary) if error_message_summary else None
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details) 
    return results

async def process_adjust_in_item(skus, quantities, lots, dates, locations, reasons, user_id, username, id_stamp, raw_command, parsed_details):
    """Processes a single inventory adjustment or multiple items from /adjust_in command."""
    results = []
    overall_success = True
    error_message_summary = []

    log_transaction_to_file(id_stamp, "adjust_in", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing adjustment items")

    try:
        for sku, qty_str, lot, date_str, loc, reason in zip(skus, quantities, lots, dates, locations, reasons):
            item_success = True
            item_error = None
            try:
                adjustment_quantity = int(qty_str) # Can be positive or negative
                
                try:
                    record_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    results.append(f"❌ SKU: {sku} - รูปแบบวันที่ไม่ถูกต้อง (YYYY-MM-DD)")
                    item_success = False
                    item_error = "Invalid date format"
                    continue

                check_query = "SELECT quantity FROM inventory WHERE sku = %s AND lot = %s AND location = %s"
                existing_record = DB_CONNECTOR.execute_query(check_query, (sku, lot, loc), fetch_one=True)

                if existing_record:
                    current_quantity = existing_record[0]
                    new_quantity = current_quantity + adjustment_quantity
                    
                    if new_quantity < 0:
                        results.append(f"❌ SKU: {sku} - จำนวนใหม่ติดลบ ({new_quantity}) หลังการปรับปรุง")
                        item_success = False
                        item_error = f"Negative quantity ({new_quantity}) after adjustment"
                        continue

                    update_query = """
                    UPDATE inventory SET quantity = %s, last_updated_date = %s, last_updated_by = %s, id_stamp = %s
                    WHERE sku = %s AND lot = %s AND location = %s
                    """
                    DB_CONNECTOR.execute_query(update_query, 
                                               (new_quantity, datetime.now(), user_id, id_stamp, sku, lot, loc), 
                                               commit=True)
                    
                    results.append(f"✅ SKU: {sku}, จำนวนที่ปรับ: {adjustment_quantity}, Lot: {lot}, Loc: {loc} - จำนวนใหม่: {new_quantity} (เหตุผล: {reason})")
                    logger.info(f"[{id_stamp}] Adjusted inventory for SKU {sku}, Lot {lot}, Loc {loc} by {adjustment_quantity}. New quantity: {new_quantity}. Reason: {reason}. By User: {user_id}")
                else:
                    results.append(f"❌ SKU: {sku}, Lot: {lot}, Loc: {loc} - ไม่พบรายการสินค้าในคลังเพื่อปรับปรุง")
                    logger.warning(f"[{id_stamp}] Adjustment failed: SKU {sku}, Lot {lot}, Loc {loc} not found. By User: {user_id}")
                    item_success = False
                    item_error = "Item not found for adjustment"

            except ValueError as ve:
                results.append(f"❌ SKU: {sku} - จำนวนไม่ถูกต้อง: {ve}")
                logger.warning(f"[{id_stamp}] Invalid quantity for adjustment SKU {sku}. Error: {ve}. User: {user_id}.")
                item_success = False
                item_error = f"ValueError: {ve}"
            except Exception as e:
                results.append(f"❌ SKU: {sku} - เกิดข้อผิดพลาด: {e}")
                logger.error(f"[{id_stamp}] Error processing /adjust_in for SKU {sku}. Error: {e}")
                item_success = False
                item_error = f"General error: {e}"
            
            if not item_success:
                overall_success = False
                error_message_summary.append(f"SKU {sku} (Lot: {lot}, Loc: {loc}): {item_error}")
    except Exception as e:
        logger.error(f"[{id_stamp}] General error in process_adjust_in_item: {e}")
        results.append(f"❌ เกิดข้อผิดพลาดทั่วไปในการประมวลผล: {e}")
        overall_success = False
        error_message_summary.append(f"Overall processing error: {e}")
    
    final_status = "SUCCESS" if overall_success and not error_message_summary else "FAILED"
    final_message = "ปรับปรุงสินค้าสำเร็จทุกรายการ" if overall_success and not error_message_summary else "ปรับปรุงสินค้าล้มเหลวบางรายการ"
    error_details = ", ".join(error_message_summary) if error_message_summary else None
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details) 
    return results

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message when the command /start is issued."""
    id_stamp = generate_id_stamp("START")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id) # Use ID if no username
    raw_command = update.message.text
    
    # Log initial transaction status as PROCESSING
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
        f"สวัสดีครับ! ยินดีต้อนรับสู่ Bot1 (ระบบรับสินค้า).\n"
        f"คุณสามารถใช้คำสั่งต่อไปนี้:\n"
        f"/in (SKU) (จำนวน) (LOT) (วันที่) (Location) (รองรับหลายรายการ)\n"
        f"/return (SKU) (จำนวน) (LOT) (วันที่) (Location) (รองรับหลายรายการ)\n"
        f"/adjust_in (SKU) (จำนวน) (LOT) (วันที่) (Location) (เหตุผล) (รองรับหลายรายการ)\n"
        f"Transaction ID: {id_stamp}"
    )

async def handle_in_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /in command for inbound inventory."""
    id_stamp = generate_id_stamp("IN")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args 
    
    # We log initial state within the process_ function for multi-item commands
    # because parsed_details might be needed, and we want to ensure it's handled consistently.
    # The actual log_transaction_to_file call for "PROCESSING" will be inside process_inbound_item.
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /in command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        # Log unauthorized access directly here if it bypasses process function
        log_transaction_to_file(id_stamp, "in", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /in by user {user_id}.")
        return

    parsed_items = parse_multi_param_command(" ".join(command_args), 5)

    if not parsed_items:
        log_transaction_to_file(id_stamp, "in", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format: No items parsed.", "Command arguments could not be parsed into items.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /in SKU001 10 LOT1 2025-05-29 LOCA (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /in command format from user {user_id}. No items parsed.")
        return

    skus = [item[0] for item in parsed_items]
    quantities = [item[1] for item in parsed_items]
    lots = [item[2] for item in parsed_items]
    dates = [item[3] for item in parsed_items]
    locations = [item[4] for item in parsed_items]

    results = await process_inbound_item(skus, quantities, lots, dates, locations, user_id, username, id_stamp, full_command, parsed_items)
    
    reply_message = "\n".join(results)
    if len(results) > 1:
        success_count = sum(1 for r in results if r.startswith("✅"))
        fail_count = len(results) - success_count
        reply_message = f"สรุปผลการรับสินค้า {len(results)} รายการ:\n" \
                        f"✅ สำเร็จ: {success_count} รายการ\n" \
                        f"❌ ล้มเหลว: {fail_count} รายการ\n\n" + reply_message
    
    await update.message.reply_text(
        f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{reply_message}"
    )

async def handle_return_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /return command for returning inventory."""
    id_stamp = generate_id_stamp("RET")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /return command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "return", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /return by user {user_id}.")
        return

    parsed_items = parse_multi_param_command(" ".join(command_args), 5)

    if not parsed_items:
        log_transaction_to_file(id_stamp, "return", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format: No items parsed.", "Command arguments could not be parsed into items.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /return SKU001 5 LOT1 2025-05-29 LOCA (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /return command format from user {user_id}. No items parsed.")
        return

    skus = [item[0] for item in parsed_items]
    quantities = [item[1] for item in parsed_items]
    lots = [item[2] for item in parsed_items]
    dates = [item[3] for item in parsed_items]
    locations = [item[4] for item in parsed_items]

    results = await process_return_item(skus, quantities, lots, dates, locations, user_id, username, id_stamp, full_command, parsed_items)
    
    reply_message = "\n".join(results)
    if len(results) > 1:
        success_count = sum(1 for r in results if r.startswith("✅"))
        fail_count = len(results) - success_count
        reply_message = f"สรุปผลการคืนสินค้า {len(results)} รายการ:\n" \
                        f"✅ สำเร็จ: {success_count} รายการ\n" \
                        f"❌ ล้มเหลว: {fail_count} รายการ\n\n" + reply_message
    
    await update.message.reply_text(
        f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{reply_message}"
    )

async def handle_adjust_in_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /adjust_in command for adjusting inbound inventory."""
    id_stamp = generate_id_stamp("ADJIN")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /adjust_in command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "adjust_in", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /adjust_in by user {user_id}.")
        return

    parsed_items = parse_multi_param_command(" ".join(command_args), 6)

    if not parsed_items:
        log_transaction_to_file(id_stamp, "adjust_in", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format: No items parsed.", "Command arguments could not be parsed into items.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /adjust_in SKU001 5 LOT1 2025-05-29 LOCA เหตุผล (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /adjust_in command format from user {user_id}. No items parsed.")
        return

    skus = [item[0] for item in parsed_items]
    quantities = [item[1] for item in parsed_items]
    lots = [item[2] for item in parsed_items]
    dates = [item[3] for item in parsed_items]
    locations = [item[4] for item in parsed_items]
    reasons = [item[5] for item in parsed_items]

    results = await process_adjust_in_item(skus, quantities, lots, dates, locations, reasons, user_id, username, id_stamp, full_command, parsed_items)
    
    reply_message = "\n".join(results)
    if len(results) > 1:
        success_count = sum(1 for r in results if r.startswith("✅"))
        fail_count = len(results) - success_count
        reply_message = f"สรุปผลการปรับปรุงสินค้า {len(results)} รายการ:\n" \
                        f"✅ สำเร็จ: {success_count} รายการ\n" \
                        f"❌ ล้มเหลว: {fail_count} รายการ\n\n" + reply_message
    
    await update.message.reply_text(
        f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{reply_message}"
    )

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
    application.add_handler(CommandHandler("in", handle_in_command))
    application.add_handler(CommandHandler("return", handle_return_command))
    application.add_handler(CommandHandler("adjust_in", handle_adjust_in_command))

    # Register handler for unknown commands
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    # Register error handler
    application.add_error_handler(error_handler)

    logger.info(f"Bot1 is polling...")
    application.run_polling()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot1 stopped by user.")
    except Exception as e:
        logger.critical(f"Bot1 encountered a critical error and stopped: {e}")
    finally:
        if DB_CONNECTOR:
            DB_CONNECTOR.disconnect()
            logger.info("Bot1 database disconnected.")