import asyncio
import threading
import subprocess
import os
import sys
import logging

# Set up a basic logger for the main runner
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# List of bots and their main script paths
bots_to_run = {
    "bot1": os.path.join("Main", "bot1", "main.py"),
    "bot2": os.path.join("Main", "bot2", "main.py"),
    "bot3": os.path.join("Main", "bot3", "main.py"),
    "bot4": os.path.join("Main", "bot4", "main.py"),
}

def run_bot_in_subprocess(bot_id, script_path):
    """Runs a single bot in a separate subprocess."""
    logger.info(f"Starting {bot_id} in a separate subprocess...")
    try:
        # Use Popen to run without waiting for it to finish
        # This will run the bot's main.py which itself uses application.run_polling()
        process = subprocess.Popen([sys.executable, script_path])
        process.wait() # This will make the main runner wait for the bot process to finish
                      # For continuous running, you might need a proper process manager
                      # or handle subprocesses more carefully if you want to keep them alive
                      # and restart them upon failure from this script.
                      # For simple 'start all and keep running', this Popen is sufficient.
    except Exception as e:
        logger.error(f"Error starting {bot_id}: {e}")

async def main_runner_async():
    """Main async function to manage bot lifecycles (conceptual)."""
    # This example uses threading to run each bot's main.py in its own thread
    # because `application.run_polling()` is blocking.
    # For truly asynchronous Telegram bots within one Python process,
    # you'd need to refactor each bot's `main.py` to use a shared `Application` instance
    # and add all handlers to it, rather than each bot having its own `ApplicationBuilder().build()`.
    # But for keeping the current `main.py` structure, threading is simpler.

    logger.info("Starting all bots using threading...")
    threads = []
    for bot_id, script_path in bots_to_run.items():
        # Using a lambda to pass arguments to the target function
        thread = threading.Thread(target=run_bot_in_subprocess, args=(bot_id, script_path))
        thread.daemon = True # Allow main program to exit even if threads are running
        threads.append(thread)
        thread.start()
    
    # Keep the main thread alive. In a real application, you might have a graceful shutdown mechanism.
    try:
        while True:
            await asyncio.sleep(3600) # Sleep for a long time, or until interrupted
    except asyncio.CancelledError:
        logger.info("Main runner cancelled.")
    except KeyboardInterrupt:
        logger.info("Main runner interrupted by user.")
    finally:
        logger.info("Main runner shutting down.")
        # In a real system, you'd send a signal to gracefully stop subprocesses here.
        # For now, daemon threads will exit when the main program exits.

if __name__ == "__main__":
    # Change to the base directory if running this script from somewhere else
    # Make sure to run this script from the project root where 'Main/' folder is.
    if not os.path.exists('Main'):
        logger.error("Please run this script from the root directory of your project (where 'Main/' folder is located).")
        sys.exit(1)

    # Initialize the database for all bots (only needs to be done once)
    # The db_connector.py itself will create tables on first connect.
    try:
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'Main', 'Database')))
        from db_connector import DatabaseConnector
        db_init = DatabaseConnector()
        db_init.connect()
        db_init.disconnect() # Disconnect after ensuring tables are created
        logger.info("Database connection and table creation check completed.")
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}. Exiting.")
        sys.exit(1)

    # Run the async main runner
    try:
        asyncio.run(main_runner_async())
    except KeyboardInterrupt:
        logger.info("All bots stopped by user.")
    except Exception as e:
        logger.critical(f"A critical error occurred in the main runner: {e}")