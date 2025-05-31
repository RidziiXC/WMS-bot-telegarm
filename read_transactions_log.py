import json
import os
import sys

# Define the path to the transaction log file
# This path is relative to where read_transactions_log.py is run.
# It assumes read_transactions_log.py is in the project root,
# and the log file is in Main/Log/transactions.log
TRANSACTION_LOG_FILE = os.path.join("Main", "Log", "transactions.log")

def read_and_filter_transactions(search_id_stamp=None, search_command_type=None, search_user_id=None, status_filter=None):
    """
    Reads the transactions.log file and filters entries based on provided criteria.
    """
    results = []
    if not os.path.exists(TRANSACTION_LOG_FILE):
        print(f"Error: Transaction log file not found at {TRANSACTION_LOG_FILE}")
        return results

    print(f"Reading transaction log from: {TRANSACTION_LOG_FILE}")
    try:
        with open(TRANSACTION_LOG_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    
                    # Apply filters
                    if search_id_stamp and entry.get("id_stamp") != search_id_stamp:
                        continue
                    if search_command_type and entry.get("command_type") != search_command_type:
                        continue
                    # Ensure user_id comparison handles both int from input and int from log
                    if search_user_id is not None: 
                        if entry.get("user_id") != search_user_id:
                            continue
                    if status_filter:
                        # Check both initial status and status_update
                        current_status = entry.get("status")
                        updated_status = entry.get("status_update")
                        if current_status != status_filter and updated_status != status_filter:
                            continue
                        
                    results.append(entry)
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse line as JSON: {line.strip()}. Error: {e}")
                except Exception as e:
                    print(f"Warning: Unexpected error processing log entry: {e}. Entry: {line.strip()}")
    except Exception as e:
        print(f"Error reading transaction log file: {e}")
    return results

if __name__ == "__main__":
    while True: # Loop to allow multiple searches without restarting the script
        print("\n--- Transaction Log Viewer ---")
        print("Options:")
        print("1. View all logs")
        print("2. Search by ID Stamp")
        print("3. Search by Command Type")
        print("4. Search by User ID")
        print("5. Search by Status (PROCESSING, SUCCESS, FAILED, PENDING_CONFIRMATION, CANCELLED)")
        print("6. Exit")

        choice = input("Enter your choice (1-6): ").strip()
        
        filtered_logs = []
        
        if choice == '1':
            filtered_logs = read_and_filter_transactions()
            print("\n--- All Transaction Logs ---")
        elif choice == '2':
            id_stamp = input("Enter ID Stamp to search: ").strip()
            filtered_logs = read_and_filter_transactions(search_id_stamp=id_stamp)
            print(f"\n--- Logs for ID Stamp: {id_stamp} ---")
        elif choice == '3':
            cmd_type = input("Enter Command Type (e.g., in, out, reserve, start, unknown, error): ").strip()
            filtered_logs = read_and_filter_transactions(search_command_type=cmd_type)
            print(f"\n--- Logs for Command Type: {cmd_type} ---")
        elif choice == '4':
            try:
                user_id_str = input("Enter User ID to search: ").strip()
                user_id = int(user_id_str)
                filtered_logs = read_and_filter_transactions(search_user_id=user_id)
                print(f"\n--- Logs for User ID: {user_id} ---")
            except ValueError:
                print("Invalid User ID. Please enter a number.")
                continue # Continue the loop to show menu again
        elif choice == '5':
            status = input("Enter Status (e.g., SUCCESS, FAILED, PROCESSING, PENDING_CONFIRMATION, CANCELLED): ").strip().upper()
            filtered_logs = read_and_filter_transactions(status_filter=status)
            print(f"\n--- Logs for Status: {status} ---")
        elif choice == '6':
            print("Exiting Log Viewer.")
            break # Exit the while loop
        else:
            print("Invalid choice. Please enter a number between 1 and 6.")
            continue # Continue the loop to show menu again

        if filtered_logs:
            for log in filtered_logs:
                # Pretty print JSON for readability
                print(json.dumps(log, indent=2, ensure_ascii=False)) 
                print("-" * 20)
        else:
            if choice not in ['4', '6']: # Avoid printing "No matching logs" if input was invalid or exiting
                print("No matching logs found.")
        
        # This input ensures the console stays open after displaying results, unless exiting
        if choice != '6': # Don't ask for input if user chose to exit
            input("\nPress Enter to return to menu...") 

    # Final input to keep console open if program is run by double-click etc.
    # This will only be reached if the loop breaks (e.g., choice '6')
    input("\nViewer session ended. Press Enter to close this window...")