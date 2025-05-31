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
# No longer need set_db_connector as transaction logs are in files
from Function.utils import load_config, setup_logging, generate_id_stamp, is_user_allowed, parse_multi_param_command, log_transaction_to_file, update_transaction_log_file_status
# Import database connector (still needed for inventory and reservations)
from Database.db_connector import DatabaseConnector

# --- Global Variables & Initialization ---
BOT_ID = "bot2"
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

    BOT_TOKEN = tokens.get("BOT2_TOKEN")
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_2":
        logging.error("BOT2_TOKEN not found or is placeholder. Please update Config/token.json")
        sys.exit(1)

    # Setup logging for Bot2
    log_level = config.get("LOGGING_CONFIG", {}).get("LEVEL", "INFO").upper()
    logger = setup_logging(BOT_ID, log_level)
    logger.info(f"--- Starting Bot2: {BOT_ID} ---")

    # Initialize Database Connector (still needed for inventory and reservations tables)
    DB_CONNECTOR = DatabaseConnector()
    # No need to set_db_connector for utils as transaction logs are to file.

    try:
        DB_CONNECTOR.connect()
        logger.info("Bot2 successfully connected to database.")
    except Exception as e:
        logger.error(f"Bot2 failed to connect to database: {e}. Some functionalities might be impaired.")

    INVENTORY_SETTINGS = config.get("INVENTORY_SETTINGS", {})

except Exception as e:
    logging.critical(f"Critical error during Bot2 initialization: {e}")
    sys.exit(1)

# --- Database Interaction Functions ---

async def process_outbound_items(skus, quantities, reasons, user_id, username, id_stamp, raw_command, parsed_details):
    """Processes multiple outbound items from /out command."""
    results = []
    overall_success = True
    error_message_summary = []
    
    log_transaction_to_file(id_stamp, "out", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing outbound items")

    try:
        for sku, qty_str, reason in zip(skus, quantities, reasons):
            item_success = True
            item_error = None
            try:
                quantity = int(qty_str)
                if quantity <= 0:
                    results.append(f"❌ SKU: {sku} - จำนวนต้องเป็นบวก")
                    item_success = False
                    item_error = "Invalid quantity (non-positive)"
                    continue

                check_stock_query = "SELECT lot, location, quantity FROM inventory WHERE sku = %s AND quantity > 0 ORDER BY inbound_date ASC"
                available_stock = DB_CONNECTOR.execute_query(check_stock_query, (sku,), fetch_all=True)

                if not available_stock:
                    results.append(f"❌ SKU: {sku} - ไม่พบสินค้าในคลัง")
                    logger.warning(f"[{id_stamp}] No stock found for SKU {sku}. User: {user_id}")
                    item_success = False
                    item_error = "No stock found"
                    continue

                total_available = sum(s[2] for s in available_stock)
                if total_available < quantity:
                    results.append(f"❌ SKU: {sku} - สินค้าไม่พอ. มีในคลัง {total_available} ชิ้น ต้องการ {quantity} ชิ้น")
                    logger.warning(f"[{id_stamp}] Insufficient stock for SKU {sku}. Available: {total_available}, Requested: {quantity}. User: {user_id}")
                    item_success = False
                    item_error = "Insufficient stock"
                    continue

                # Perform stock deduction (simplified FIFO here)
                remaining_to_deduct = quantity
                deducted_details = []

                for lot, loc, current_qty in available_stock:
                    if remaining_to_deduct <= 0:
                        break

                    qty_to_deduct_from_lot = min(remaining_to_deduct, current_qty)
                    new_qty_in_lot = current_qty - qty_to_deduct_from_lot

                    update_query = """
                    UPDATE inventory SET quantity = %s, last_updated_date = %s, last_updated_by = %s, id_stamp = %s
                    WHERE sku = %s AND lot = %s AND location = %s
                    """
                    DB_CONNECTOR.execute_query(update_query, 
                                               (new_qty_in_lot, datetime.now(), user_id, id_stamp, sku, lot, loc), 
                                               commit=True)
                    deducted_details.append(f"ตัด {qty_to_deduct_from_lot} ชิ้น จาก Lot: {lot}, Loc: {loc}")
                    remaining_to_deduct -= qty_to_deduct_from_lot
                
                results.append(f"✅ SKU: {sku}, จำนวน: {quantity}, เหตุผล: {reason} - ตัดสินค้าออกแล้ว ({'; '.join(deducted_details)})")
                logger.info(f"[{id_stamp}] Outbound for SKU {sku}, Qty {quantity}. Reason: {reason}. Deducted from: {deducted_details}. By User: {user_id}")

            except ValueError as ve:
                results.append(f"❌ SKU: {sku} - จำนวนไม่ถูกต้อง: {ve}")
                logger.warning(f"[{id_stamp}] Invalid quantity for outbound SKU {sku}. Error: {ve}. User: {user_id}.")
                item_success = False
                item_error = f"ValueError: {ve}"
            except Exception as e:
                results.append(f"❌ SKU: {sku} - เกิดข้อผิดพลาด: {e}")
                logger.error(f"[{id_stamp}] Error processing /out for SKU {sku}. Error: {e}")
                item_success = False
                item_error = f"General error: {e}"
            
            if not item_success:
                overall_success = False
                error_message_summary.append(f"SKU {sku}: {item_error}")

    except Exception as e:
        logger.error(f"[{id_stamp}] General error in process_outbound_items: {e}")
        results.append(f"❌ เกิดข้อผิดพลาดทั่วไปในการประมวลผล: {e}")
        overall_success = False
        error_message_summary.append(f"Overall processing error: {e}")
    
    final_status = "SUCCESS" if overall_success and not error_message_summary else "FAILED"
    final_message = "ส่งออกสินค้าสำเร็จทุกรายการ" if overall_success and not error_message_summary else "ส่งออกสินค้าล้มเหลวบางรายการ"
    error_details = ", ".join(error_message_summary) if error_message_summary else None
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details)
    return results

async def process_cancel_out_item(sku, quantity, reason, user_id, username, id_stamp, raw_command, parsed_details):
    """Processes cancellation of an outbound item (adds back to stock)."""
    overall_success = True
    error_message = None
    return_message = ""
    
    log_transaction_to_file(id_stamp, "cancel_out", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing cancel_out item")

    try:
        qty = int(quantity)
        if qty <= 0:
            overall_success = False
            error_message = "จำนวนต้องเป็นบวก"
            return_message = f"❌ {error_message}"
        else:
            check_query = "SELECT lot, location FROM inventory WHERE sku = %s LIMIT 1"
            existing_loc_lot = DB_CONNECTOR.execute_query(check_query, (sku,), fetch_one=True)

            target_lot = existing_loc_lot[0] if existing_loc_lot else "CANCELLED_LOT"
            target_loc = existing_loc_lot[1] if existing_loc_lot else "CANCELLED_LOC"

            check_exact_query = "SELECT quantity FROM inventory WHERE sku = %s AND lot = %s AND location = %s"
            exact_record = DB_CONNECTOR.execute_query(check_exact_query, (sku, target_lot, target_loc), fetch_one=True)

            if exact_record:
                new_quantity = exact_record[0] + qty
                update_query = """
                UPDATE inventory SET quantity = %s, last_updated_date = %s, last_updated_by = %s, id_stamp = %s
                WHERE sku = %s AND lot = %s AND location = %s
                """
                DB_CONNECTOR.execute_query(update_query, 
                                           (new_quantity, datetime.now(), user_id, id_stamp, sku, target_lot, target_loc), 
                                           commit=True)
                logger.info(f"[{id_stamp}] Cancelled outbound for SKU {sku}, Qty {qty}. New Qty in {target_loc}: {new_quantity}. Reason: {reason}. By User: {user_id}")
                return_message = f"✅ SKU: {sku}, จำนวน: {qty}, เหตุผล: {reason} - ยกเลิกรายการส่งออกแล้ว (เพิ่มกลับที่ {target_loc}, Lot {target_lot})"
            else:
                insert_query = """
                INSERT INTO inventory (sku, lot, location, quantity, inbound_date, created_by, id_stamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                DB_CONNECTOR.execute_query(insert_query, 
                                           (sku, target_lot, target_loc, qty, datetime.now().date(), user_id, id_stamp), 
                                           commit=True)
                logger.info(f"[{id_stamp}] Cancelled outbound for SKU {sku}, Qty {qty}. Inserted as new stock at {target_loc}, Lot {target_lot}. Reason: {reason}. By User: {user_id}")
                return_message = f"✅ SKU: {sku}, จำนวน: {qty}, เหตุผล: {reason} - ยกเลิกรายการส่งออกและบันทึกเป็นสินค้าใหม่ที่ {target_loc}, Lot {target_lot}"

    except ValueError as ve:
        overall_success = False
        error_message = f"จำนวนไม่ถูกต้อง: {ve}"
        return_message = f"❌ {error_message}"
        logger.warning(f"[{id_stamp}] Invalid quantity for /cancel_out SKU {sku}. Error: {ve}. User: {user_id}.")
    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาด: {e}"
        return_message = f"❌ เกิดข้อผิดพลาดในการยกเลิกรายการส่งออก: {e}"
        logger.error(f"[{id_stamp}] Error processing /cancel_out for SKU {sku}. Error: {e}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = return_message.replace("✅ ", "") if overall_success else return_message.replace("❌ ", "")
    error_details = error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details)
    return return_message


async def process_reserve_item(sku, quantity, user_id, username, id_stamp, raw_command, parsed_details):
    """Processes a reservation for an SKU."""
    overall_success = True
    error_message = None
    reservation_id = None
    return_message = ""
    
    log_transaction_to_file(id_stamp, "reserve", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing reservation")

    try:
        qty = int(quantity)
        if qty <= 0:
            overall_success = False
            error_message = "จำนวนต้องเป็นบวก"
            return_message = f"❌ {error_message}"
        else:
            check_stock_query = "SELECT lot, location, quantity, inbound_date FROM inventory WHERE sku = %s AND quantity > 0 ORDER BY inbound_date ASC"
            available_stock = DB_CONNECTOR.execute_query(check_stock_query, (sku,), fetch_all=True)

            if not available_stock:
                overall_success = False
                error_message = f"SKU: {sku} - ไม่พบสินค้าในคลังเพื่อจอง"
                return_message = f"❌ {error_message}"
            
            total_available = sum(s[2] for s in available_stock)
            if total_available < qty:
                overall_success = False
                error_message = f"SKU: {sku} - สินค้าไม่พอจอง. มีในคลัง {total_available} ชิ้น ต้องการจอง {qty} ชิ้น"
                return_message = f"❌ {error_message}"
            
            # Deduct from inventory and create a reservation record
            remaining_to_reserve = qty
            reserved_lots_locs = []
            reservation_id = generate_id_stamp("RES") # New ID for the reservation itself

            for lot, loc, current_qty, inbound_date in available_stock:
                if remaining_to_reserve <= 0:
                    break

                qty_to_reserve_from_lot = min(remaining_to_reserve, current_qty)
                new_qty_in_lot = current_qty - qty_to_reserve_from_lot

                update_query = """
                UPDATE inventory SET quantity = %s, last_updated_date = %s, last_updated_by = %s, id_stamp = %s
                WHERE sku = %s AND lot = %s AND location = %s
                """
                DB_CONNECTOR.execute_query(update_query, 
                                           (new_qty_in_lot, datetime.now(), user_id, id_stamp, sku, lot, loc), 
                                           commit=True)
                
                reserved_lots_locs.append({"lot": lot, "location": loc, "quantity": qty_to_reserve_from_lot})
                remaining_to_reserve -= qty_to_reserve_from_lot
            
            # Insert reservation record
            insert_reserve_query = """
            INSERT INTO reservations (reserve_id, sku, quantity, reserved_by, reservation_date, status, reserved_lot, reserved_location, id_stamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            first_reserved_lot = reserved_lots_locs[0]['lot'] if reserved_lots_locs else None
            first_reserved_loc = reserved_lots_locs[0]['location'] if reserved_lots_locs else None

            DB_CONNECTOR.execute_query(insert_reserve_query,
                                       (reservation_id, sku, qty, user_id, datetime.now(), 'PENDING', 
                                        first_reserved_lot, first_reserved_loc, id_stamp),
                                       commit=True)
            
            details_str = ", ".join([f"Lot: {d['lot']}, Loc: {d['location']}, Qty: {d['quantity']}" for d in reserved_lots_locs])
            logger.info(f"[{id_stamp}] Reserved SKU {sku}, Qty {qty}. ReserveID: {reservation_id}. Details: {details_str}. By User: {user_id}")
            return_message = f"✅ SKU: {sku}, จำนวน: {qty} - จองแล้ว. Reserve ID: `{reservation_id}`. รายละเอียด: {details_str}. (Transaction ID: {id_stamp})"

    except ValueError as ve:
        overall_success = False
        error_message = f"จำนวนไม่ถูกต้อง: {ve}"
        return_message = f"❌ {error_message}"
        logger.warning(f"[{id_stamp}] Invalid quantity for /reserve SKU {sku}. Error: {ve}. User: {user_id}.")
    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดในการจองสินค้า: {e}"
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] Error processing /reserve for SKU {sku}. Error: {e}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = return_message.replace("✅ ", "").split(". (Transaction ID")[0] if overall_success else return_message.replace("❌ ", "")
    error_details = error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details)
    return return_message


async def process_reserve_pick(sku, quantity, location, lot, reserve_id, user_id, username, id_stamp, raw_command, parsed_details):
    """Confirms picking of a reserved item."""
    overall_success = True
    error_message = None
    return_message = ""
    
    log_transaction_to_file(id_stamp, "reserve_pick", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing reserve pick")

    try:
        qty = int(quantity)
        if qty <= 0:
            overall_success = False
            error_message = "จำนวนต้องเป็นบวก"
            return_message = f"❌ {error_message}"
        else:
            check_reserve_query = """
            SELECT quantity, status FROM reservations
            WHERE reserve_id = %s AND sku = %s AND reserved_lot = %s AND reserved_location = %s AND status = 'PENDING'
            """
            reserve_record = DB_CONNECTOR.execute_query(check_reserve_query, (reserve_id, sku, lot, location), fetch_one=True)

            if not reserve_record:
                overall_success = False
                error_message = f"ไม่พบรายการจอง Reserve ID: {reserve_id} สำหรับ SKU: {sku} หรือสถานะไม่ถูกต้อง."
                return_message = f"❌ {error_message}"
            else:
                reserved_qty = reserve_record[0]
                if qty > reserved_qty:
                    overall_success = False
                    error_message = f"จำนวนที่หยิบ ({qty}) เกินกว่าจำนวนที่จองไว้ ({reserved_qty})."
                    return_message = f"❌ {error_message}"
                else:
                    update_reserve_query = """
                    UPDATE reservations SET status = 'PICKED', pickup_date = %s, picked_by = %s
                    WHERE reserve_id = %s
                    """
                    DB_CONNECTOR.execute_query(update_reserve_query, (datetime.now(), user_id, reserve_id), commit=True)
                    
                    logger.info(f"[{id_stamp}] Reserved item PICKED: ReserveID {reserve_id}, SKU {sku}, Qty {qty}, Lot {lot}, Loc {location}. By User: {user_id}")
                    return_message = f"✅ SKU: {sku}, จำนวน: {qty} - หยิบสินค้าจากการจอง `{reserve_id}` ที่ Loc: {location}, Lot: {lot} แล้ว"

    except ValueError as ve:
        overall_success = False
        error_message = f"จำนวนไม่ถูกต้อง: {ve}"
        return_message = f"❌ {error_message}"
        logger.warning(f"[{id_stamp}] Invalid quantity for /reserve_pick. Error: {ve}. User: {user_id}.")
    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดในการยืนยันการหยิบสินค้า: {e}"
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] Error processing /reserve_pick for ReserveID {reserve_id}. Error: {e}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = return_message.replace("✅ ", "") if overall_success else return_message.replace("❌ ", "")
    error_details = error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details)
    return return_message

async def process_reserve_return(sku, quantity, location, lot, reserve_id, reason, user_id, username, id_stamp, raw_command, parsed_details):
    """Processes returning a reserved item to stock."""
    overall_success = True
    error_message = None
    return_message = ""
    
    log_transaction_to_file(id_stamp, "reserve_return", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing reserve return")

    try:
        qty = int(quantity)
        if qty <= 0:
            overall_success = False
            error_message = "จำนวนต้องเป็นบวก"
            return_message = f"❌ {error_message}"
        else:
            check_reserve_query = """
            SELECT quantity, status FROM reservations
            WHERE reserve_id = %s AND sku = %s AND reserved_lot = %s AND reserved_location = %s
            """
            reserve_record = DB_CONNECTOR.execute_query(check_reserve_query, (reserve_id, sku, lot, location), fetch_one=True)

            if not reserve_record:
                overall_success = False
                error_message = f"ไม่พบรายการจอง Reserve ID: {reserve_id} สำหรับ SKU: {sku} หรือข้อมูลไม่ตรงกัน."
                return_message = f"❌ {error_message}"
            else:
                # Add quantity back to inventory
                update_inventory_query = """
                UPDATE inventory SET quantity = quantity + %s, last_updated_date = %s, last_updated_by = %s, id_stamp = %s
                WHERE sku = %s AND lot = %s AND location = %s
                """
                DB_CONNECTOR.execute_query(update_inventory_query, 
                                           (qty, datetime.now(), user_id, id_stamp, sku, lot, location), 
                                           commit=True)
                
                # Update reservation status (if not already cancelled/picked)
                update_reserve_status_query = """
                UPDATE reservations SET status = 'RETURNED', return_date = %s, returned_by = %s, return_reason = %s
                WHERE reserve_id = %s
                """
                DB_CONNECTOR.execute_query(update_reserve_status_query, (datetime.now(), user_id, reason, reserve_id), commit=True)

                logger.info(f"[{id_stamp}] Reserved item RETURNED: ReserveID {reserve_id}, SKU {sku}, Qty {qty}, Lot {lot}, Loc {location}. Reason: {reason}. By User: {user_id}")
                return_message = f"✅ SKU: {sku}, จำนวน: {qty} - สินค้าที่จอง `{reserve_id}` ถูกคืนเข้าระบบแล้ว (เหตุผล: {reason})"

    except ValueError as ve:
        overall_success = False
        error_message = f"จำนวนไม่ถูกต้อง: {ve}"
        return_message = f"❌ {error_message}"
        logger.warning(f"[{id_stamp}] Invalid quantity for /reserve_return. Error: {ve}. User: {user_id}.")
    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดในการคืนสินค้าที่จอง: {e}"
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] Error processing /reserve_return for ReserveID {reserve_id}. Error: {e}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = return_message.replace("✅ ", "") if overall_success else return_message.replace("❌ ", "")
    error_details = error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details)
    return return_message

async def process_reserve_cancel(reserve_id, reason, user_id, username, id_stamp, raw_command, parsed_details):
    """Cancels a pending reservation and returns items to stock."""
    overall_success = True
    error_message = None
    return_message = ""
    
    log_transaction_to_file(id_stamp, "reserve_cancel", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing reserve cancel")

    try:
        get_reserve_details_query = """
        SELECT sku, quantity, reserved_lot, reserved_location FROM reservations
        WHERE reserve_id = %s AND status = 'PENDING'
        """
        reserve_record = DB_CONNECTOR.execute_query(get_reserve_details_query, (reserve_id,), fetch_one=True)

        if not reserve_record:
            overall_success = False
            error_message = f"ไม่พบรายการจอง Reserve ID: {reserve_id} ที่อยู่ในสถานะ 'PENDING' เพื่อยกเลิก"
            return_message = f"❌ {error_message}"
        else:
            sku, quantity, lot, location = reserve_record

            update_inventory_query = """
            UPDATE inventory SET quantity = quantity + %s, last_updated_date = %s, last_updated_by = %s, id_stamp = %s
            WHERE sku = %s AND lot = %s AND location = %s
            """
            DB_CONNECTOR.execute_query(update_inventory_query, 
                                       (quantity, datetime.now(), user_id, id_stamp, sku, lot, location), 
                                       commit=True)
            
            update_reserve_query = """
            UPDATE reservations SET status = 'CANCELLED', cancel_date = %s, cancelled_by = %s, cancel_reason = %s
            WHERE reserve_id = %s
            """
            DB_CONNECTOR.execute_query(update_reserve_query, (datetime.now(), user_id, reason, reserve_id), commit=True)

            logger.info(f"[{id_stamp}] Reservation CANCELLED: ReserveID {reserve_id}, SKU {sku}, Qty {quantity}. Items returned to stock. Reason: {reason}. By User: {user_id}")
            return_message = f"✅ การจอง `{reserve_id}` สำหรับ SKU: {sku} ถูกยกเลิกแล้ว. จำนวน {quantity} ชิ้น ถูกส่งคืนเข้าคลัง (เหตุผล: {reason})."

    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดในการยกเลิกการจอง: {e}"
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] Error processing /reserve_cancel for ReserveID {reserve_id}. Error: {e}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = return_message.replace("✅ ", "") if overall_success else return_message.replace("❌ ", "")
    error_details = error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details)
    return return_message

async def process_reserve_ck(sku_filter, user_id, username, id_stamp, raw_command, parsed_details):
    """Checks current reservations."""
    overall_success = True
    error_message = None
    return_message = ""
    
    log_transaction_to_file(id_stamp, "reserve_ck", user_id, username, raw_command, parsed_details, "PROCESSING", "Started processing reserve check")

    try:
        query = """
        SELECT reserve_id, sku, quantity, reserved_lot, reserved_location, reservation_date, status
        FROM reservations
        WHERE status = 'PENDING'
        """
        params = None
        if sku_filter:
            query += " AND sku = %s"
            params = (sku_filter,)
        
        query += " ORDER BY reservation_date ASC"

        reserved_items = DB_CONNECTOR.execute_query(query, params, fetch_all=True)

        if not reserved_items:
            return_message = f"ℹ️ ไม่พบรายการสินค้าที่ถูกจอง {'สำหรับ SKU: ' + sku_filter if sku_filter else 'ทั้งหมด'}."
            logger.info(f"[{id_stamp}] User {user_id} checked reservations {'for SKU ' + sku_filter if sku_filter else 'all'}. No items found.")
        else:
            response_lines = []
            if sku_filter:
                response_lines.append(f"รายการสินค้าที่ถูกจองสำหรับ SKU: {sku_filter}:")
            else:
                response_lines.append("รายการสินค้าที่ถูกจองทั้งหมด (สถานะ PENDING):")
            
            for item in reserved_items:
                reserve_id, sku, quantity, lot, location, res_date, status = item
                response_lines.append(
                    f"- **{sku}** (จำนวน: {quantity}) - Lot: {lot}, Loc: {location}\n"
                    f"  จองเมื่อ: {res_date.strftime('%Y-%m-%d %H:%M')}, สถานะ: {status}, Reserve ID: `{reserve_id}`"
                )
            return_message = "\n".join(response_lines)
            logger.info(f"[{id_stamp}] User {user_id} checked reservations {'for SKU ' + sku_filter if sku_filter else 'all'}. Found {len(reserved_items)} items.")

    except Exception as e:
        overall_success = False
        error_message = f"เกิดข้อผิดพลาดในการตรวจสอบรายการจอง: {e}"
        return_message = f"❌ {error_message}"
        logger.error(f"[{id_stamp}] Error processing /reserve_CK. Error: {e}")
    
    final_status = "SUCCESS" if overall_success else "FAILED"
    final_message = return_message.replace("✅ ", "").split(". (Transaction ID")[0] if overall_success and not return_message.startswith("❌") else return_message.replace("❌ ", "")
    error_details = error_message
    update_transaction_log_file_status(id_stamp, final_status, final_message, error_details)
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
    
    update_transaction_log_file_status(id_stamp, "SUCCESS", "Welcome message sent")
    await update.message.reply_text(
        f"สวัสดีครับ! ยินดีต้อนรับสู่ Bot2 (ระบบส่งออกสินค้า).\n"
        f"คุณสามารถใช้คำสั่งต่อไปนี้:\n"
        f"/out (SKU) (จำนวน) (เหตุผล) (รองรับหลายรายการ)\n"
        f"/cancel_out (SKU) (จำนวน) (เหตุผล)\n"
        f"/reserve (SKU) (จำนวน)\n"
        f"/reserve_pick (SKU) (จำนวน) (Location) (Lot) (ReserveID)\n"
        f"/reserve_return (SKU) (จำนวน) (Location) (Lot) (ReserveID) (เหตุผล)\n"
        f"/reserve_cancel (ReserveID) (เหตุผล)\n"
        f"/reserve_CK [SKU]\n"
        f"Transaction ID: {id_stamp}"
    )

async def handle_out_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /out command for outbound inventory."""
    id_stamp = generate_id_stamp("OUT")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # log_transaction_to_file is called inside process_outbound_items for consistency with multi-param
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /out command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "out", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /out by user {user_id}.")
        return

    parsed_items = parse_multi_param_command(" ".join(command_args), 3)

    if not parsed_items:
        log_transaction_to_file(id_stamp, "out", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format: No items parsed.", "Command arguments could not be parsed into items.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /out SKU001 1 เหตุผล (รองรับหลายรายการ) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /out command format from user {user_id}. No items parsed.")
        return

    skus = [item[0] for item in parsed_items]
    quantities = [item[1] for item in parsed_items]
    reasons = [item[2] for item in parsed_items]

    results = await process_outbound_items(skus, quantities, reasons, user_id, username, id_stamp, full_command, parsed_items)
    
    reply_message = "\n".join(results)
    if len(results) > 1:
        success_count = sum(1 for r in results if r.startswith("✅"))
        fail_count = len(results) - success_count
        reply_message = f"สรุปผลการส่งออก {len(results)} รายการ:\n" \
                        f"✅ สำเร็จ: {success_count} รายการ\n" \
                        f"❌ ล้มเหลว: {fail_count} รายการ\n\n" + reply_message
    
    await update.message.reply_text(
        f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{reply_message}"
    )

async def handle_cancel_out_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /cancel_out command."""
    id_stamp = generate_id_stamp("CANOUT")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # log_transaction_to_file is called inside process_cancel_out_item
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /cancel_out command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "cancel_out", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /cancel_out by user {user_id}.")
        return

    args = context.args
    if len(args) != 3:
        log_transaction_to_file(id_stamp, "cancel_out", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /cancel_out (SKU) (จำนวน) (เหตุผล) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /cancel_out command format from user {user_id}. Args: {args}")
        return
    
    sku, quantity, reason = args
    result = await process_cancel_out_item(sku, quantity, reason, user_id, username, id_stamp, full_command, parsed_details={"sku":sku, "quantity":quantity, "reason":reason})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_reserve_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /reserve command."""
    id_stamp = generate_id_stamp("RESV")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # log_transaction_to_file is called inside process_reserve_item
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /reserve command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "reserve", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /reserve by user {user_id}.")
        return

    args = context.args
    if len(args) != 2:
        log_transaction_to_file(id_stamp, "reserve", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /reserve (SKU) (จำนวน) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /reserve command format from user {user_id}. Args: {args}")
        return
    
    sku, quantity = args
    result = await process_reserve_item(sku, quantity, user_id, username, id_stamp, full_command, parsed_details={"sku":sku, "quantity":quantity})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_reserve_pick_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /reserve_pick command."""
    id_stamp = generate_id_stamp("RESPK")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # log_transaction_to_file is called inside process_reserve_pick
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /reserve_pick command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "reserve_pick", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /reserve_pick by user {user_id}.")
        return

    args = context.args
    if len(args) != 5:
        log_transaction_to_file(id_stamp, "reserve_pick", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /reserve_pick (SKU) (จำนวน) (Location) (Lot) (ReserveID) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /reserve_pick command format from user {user_id}. Args: {args}")
        return
    
    sku, quantity, location, lot, reserve_id = args
    result = await process_reserve_pick(sku, quantity, location, lot, reserve_id, user_id, username, id_stamp, full_command, parsed_details={"sku":sku, "quantity":quantity, "location":location, "lot":lot, "reserve_id":reserve_id})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_reserve_return_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /reserve_return command."""
    id_stamp = generate_id_stamp("RETRN")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # log_transaction_to_file is called inside process_reserve_return
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /reserve_return command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "reserve_return", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /reserve_return by user {user_id}.")
        return

    args = context.args
    if len(args) != 6:
        log_transaction_to_file(id_stamp, "reserve_return", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /reserve_return (SKU) (จำนวน) (Location) (Lot) (ReserveID) (เหตุผล) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /reserve_return command format from user {user_id}. Args: {args}")
        return
    
    sku, quantity, location, lot, reserve_id, reason = args
    result = await process_reserve_return(sku, quantity, location, lot, reserve_id, reason, user_id, username, id_stamp, full_command, parsed_details={"sku":sku, "quantity":quantity, "location":location, "lot":lot, "reserve_id":reserve_id, "reason":reason})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_reserve_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /reserve_cancel command."""
    id_stamp = generate_id_stamp("RESCN")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # log_transaction_to_file is called inside process_reserve_cancel
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /reserve_cancel command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "reserve_cancel", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /reserve_cancel by user {user_id}.")
        return

    args = context.args
    if len(args) < 2:
        log_transaction_to_file(id_stamp, "reserve_cancel", user_id, username, full_command, {"args": command_args}, "FAILED", "Invalid command format", "Command has incorrect number of arguments.")
        await update.message.reply_text(
            f"❌ รูปแบบคำสั่งไม่ถูกต้อง. ตัวอย่าง: /reserve_cancel (ReserveID) (เหตุผล) (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Invalid /reserve_cancel command format from user {user_id}. Args: {args}")
        return
    
    reserve_id = args[0]
    reason = " ".join(args[1:])
    result = await process_reserve_cancel(reserve_id, reason, user_id, username, id_stamp, full_command, parsed_details={"reserve_id":reserve_id, "reason":reason})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}")

async def handle_reserve_ck_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /reserve_CK command."""
    id_stamp = generate_id_stamp("RESCK")
    user_id = update.effective_user.id
    username = update.effective_user.username if update.effective_user.username else str(user_id)
    full_command = update.message.text
    command_args = context.args

    # log_transaction_to_file is called inside process_reserve_ck
    
    logger.info(f"[{id_stamp}] User {user_id} ({username}) issued /reserve_CK command: {full_command}")

    if not is_user_allowed(user_id, BOT_ID):
        log_transaction_to_file(id_stamp, "reserve_ck", user_id, username, full_command, {"args": command_args}, "FAILED", "Unauthorized access", "User not allowed to use this bot.")
        await update.message.reply_text(
            f"❌ คุณไม่มีสิทธิ์ใช้งานคำสั่งนี้. (Transaction ID: {id_stamp})"
        )
        logger.warning(f"[{id_stamp}] Unauthorized access attempt for /reserve_CK by user {user_id}.")
        return

    sku_filter = args[0] if context.args else None
    result = await process_reserve_ck(sku_filter, user_id, username, id_stamp, full_command, parsed_details={"sku_filter":sku_filter})
    await update.message.reply_text(f"ผลการทำรายการ (Transaction ID: {id_stamp}):\n{result}", parse_mode='Markdown')


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
    application.add_handler(CommandHandler("out", handle_out_command))
    application.add_handler(CommandHandler("cancel_out", handle_cancel_out_command))
    application.add_handler(CommandHandler("reserve", handle_reserve_command))
    application.add_handler(CommandHandler("reserve_pick", handle_reserve_pick_command))
    application.add_handler(CommandHandler("reserve_return", handle_reserve_return_command))
    application.add_handler(CommandHandler("reserve_cancel", handle_reserve_cancel_command))
    application.add_handler(CommandHandler("reserve_ck", handle_reserve_ck_command))


    # Register handler for unknown commands
    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    # Register error handler
    application.add_error_handler(error_handler)

    logger.info(f"Bot2 is polling...")
    application.run_polling()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot2 stopped by user.")
    except Exception as e:
        logger.critical(f"Bot2 encountered a critical error and stopped: {e}")
    finally:
        if DB_CONNECTOR:
            DB_CONNECTOR.disconnect()
            logger.info("Bot2 database disconnected.")