import logging
import os
import time
from datetime import datetime

# === Create logs directory if not exists ===
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# === Dynamic log file name with timestamp ===
log_filename = f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

# === Default global logger (fallback)
logging.basicConfig(
    filename=LOG_FILE_PATH,
    filemode='w',  # Overwrite each run
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

global_logger = logging.getLogger("migration")

# Add stream handler for console output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
console_handler.setStream(open(1, 'w', encoding='utf-8', closefd=False))  # ensure emoji works in Windows terminal
global_logger.addHandler(console_handler)


class ProgressTimer:
    """
    Utility to track progress and estimate remaining time.
    """
    def __init__(self, total_records, logger=None):
        self.total = total_records
        self.logger = logger or global_logger
        self.start_time = time.time()
        self.current = 0

    def update(self):
        self.current += 1
        elapsed = time.time() - self.start_time
        avg_time = elapsed / self.current if self.current else 0
        remaining = avg_time * (self.total - self.current)
        percent_complete = (self.current / self.total) * 100 if self.total else 100

        message = (
            f"⏱️ {self.current}/{self.total} processed | "
            f"{percent_complete:.2f}% complete | "
            f"Elapsed: {elapsed:.1f}s | Est. remaining: {remaining:.1f}s"
        )
        print(message)
        self.logger.info(message)

    def stop(self):
        total_time = time.time() - self.start_time
        summary = f"🏁 Migration completed in {total_time:.2f} seconds."
        print(summary)
        self.logger.info(summary)
