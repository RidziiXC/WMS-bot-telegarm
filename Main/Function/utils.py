import uuid
from datetime import datetime # แก้ไขตรงนี้!
import json
import logging
import os
import sys

logger = logging.getLogger(__name__)

# No need for _db_connector_instance and set_db_connector if transaction logs are only in files.
# If you decide to go back to DB logging for transactions, these will need to be re-added
# and db_connector instance passed from main.py.

def generate_id_stamp(prefix="TXN"):
    """
    Generates a unique ID stamp for each transaction.
    Format: PREFIX-YYYYMMDD-HHMMSS-UUID_SHORT
    """
    timestamp_str = datetime.now().strftime("%Y%m%d-%H%M%S") # แก้ไขตรงนี้: datetime.now()
    unique_id = str(uuid.uuid4()).split('-')[0] # Use a short version of UUID
    return f"{prefix}-{timestamp_str}-{unique_id}"

def load_config(file_path):
    """Loads configuration from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {file_path}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from config file: {file_path}")
        return None

def get_allowed_user_ids(bot_id):
    """
    Loads allowed user IDs for a specific bot from users.json.
    'super_admin' role is also loaded if needed for global access.
    """
    users_config_path = os.path.join(os.path.dirname(__file__), '..', 'Config', 'users.json')
    users_config = load_config(users_config_path)
    if users_config:
        allowed_ids = set(users_config.get(bot_id, []))
        allowed_ids.update(users_config.get('super_admin', []))
        return list(allowed_ids)
    return []

def is_user_allowed(user_id, bot_id):
    """Checks if a user is allowed to use a specific bot."""
    allowed_ids = get_allowed_user_ids(bot_id)
    return user_id in allowed_ids

def parse_multi_param_command(text, expected_params_per_item):
    """
    Parses commands with multiple sets of parameters.
    """
    parts = text.split()
    items = []
    
    if not parts:
        return []

    for i in range(0, len(parts), expected_params_per_item):
        chunk = parts[i:i + expected_params_per_item]
        if len(chunk) == expected_params_per_item:
            items.append(chunk)
        else:
            logger.warning(f"Incomplete parameter set detected: {chunk}. Expected {expected_params_per_item} parameters per item. Stopping parsing for this command.")
            break 
    return items

def setup_logging(bot_name, log_level="INFO"):
    """
    Sets up logging for a specific bot.
    Logs will go to both console and a specific bot's log file.
    """
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'Log')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{bot_name}.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers[:]: # Clear existing handlers
        root_logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    logger.info(f"Logging set up for {bot_name}. Logs will be saved to {log_file}")
    return root_logger

# --- Transaction Logging Functions (now to file) ---
# This path is relative to the utils.py file
TRANSACTION_LOG_FILE = os.path.join(os.path.dirname(__file__), '..', 'Log', 'transactions.log')

def log_transaction_to_file(id_stamp, command_type, user_id, username, raw_command, parsed_details=None, status="PROCESSING", message="", error_details=None):
    """
    Logs a transaction's state to a dedicated text file.
    Each entry is a JSON object on a new line.
    """
    log_entry = {
        "id_stamp": id_stamp,
        "timestamp": datetime.now().isoformat(), # แก้ไขตรงนี้: datetime.now()
        "command_type": command_type,
        "user_id": user_id,
        "username": username,
        "status": status,
        "message": message,
        "raw_command": raw_command,
        "parsed_details": parsed_details,
        "error_details": error_details
    }
    
    try:
        # Ensure log directory exists
        os.makedirs(os.path.dirname(TRANSACTION_LOG_FILE), exist_ok=True)
        with open(TRANSACTION_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        logger.debug(f"[{id_stamp}] Logged transaction to file: {command_type} - {status}")
    except Exception as e:
        logger.error(f"[{id_stamp}] Failed to log transaction to file: {e}")

def update_transaction_log_file_status(id_stamp, status, message, error_details=None):
    """
    Logs an update to a transaction's status to the dedicated text file.
    This will append a new entry with the same id_stamp but updated status.
    """
    log_entry = {
        "id_stamp": id_stamp,
        "timestamp": datetime.now().isoformat(), # แก้ไขตรงนี้: datetime.now()
        "status_update": status, # Use a different key to distinguish from initial log
        "message": message,
        "error_details": error_details
    }
    try:
        os.makedirs(os.path.dirname(TRANSACTION_LOG_FILE), exist_ok=True)
        with open(TRANSACTION_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        logger.debug(f"[{id_stamp}] Updated transaction status in file: {status}")
    except Exception as e:
        logger.error(f"[{id_stamp}] Failed to update transaction status in file: {e}")


# Example usage (for testing utils.py individually)
if __name__ == "__main__":
    setup_logging("test_utils_file_log", "DEBUG")
    logger.info("Testing utils.py functions with file logging...")

    # Test generate_id_stamp
    id_stamp = generate_id_stamp("INB")
    logger.info(f"Generated ID Stamp: {id_stamp}")

    # Test load_config
    config_path_for_test = os.path.join(os.path.dirname(__file__), '..', 'Config', 'config.json')
    if os.path.exists(config_path_for_test):
        config = load_config(config_path_for_test)
        if config:
            logger.info(f"Loaded DB Host: {config['DATABASE_CONFIG']['HOST']}")
    else:
        logger.warning(f"Config file not found for test: {config_path_for_test}")


    # Test get_allowed_user_ids and is_user_allowed
    test_user_id = 123456789
    bot_id_test = "bot1"
    allowed_users = get_allowed_user_ids(bot_id_test)
    logger.info(f"Allowed users for {bot_id_test}: {allowed_users}")
    logger.info(f"User {test_user_id} allowed for {bot_id_test}? {is_user_allowed(test_user_id, bot_id_test)}")

    # Test parse_multi_param_command
    command_text_in = "SKU001 10 LOTA 2025-05-29 LOC1 SKU002 5 LOTB 2025-05-29 LOC2"
    parsed_items_in = parse_multi_param_command(command_text_in, 5)
    logger.info(f"Parsed /in items: {parsed_items_in}")

    # Test logging to file
    test_tx_id = generate_id_stamp("TESTLOG")
    log_transaction_to_file(test_tx_id, "test_cmd", 123, "testuser", "/test test_arg", {"args": ["test_arg"]}, "PROCESSING", "Test initiated")
    update_transaction_log_file_status(test_tx_id, "SUCCESS", "Test completed successfully")

    id_stamp_fail = generate_id_stamp("FAILTEST")
    log_transaction_to_file(id_stamp_fail, "fail_cmd", 456, "failuser", "/fail", None, "PROCESSING", "Test failed init")
    update_transaction_log_file_status(id_stamp_fail, "FAILED", "Test failed due to XYZ", "Error: XYZ reason")

    logger.info(f"Check {TRANSACTION_LOG_FILE} for logs generated by this test.")