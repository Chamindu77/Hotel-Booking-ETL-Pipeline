from loguru import logger
import os

os.makedirs("logs", exist_ok=True)

logger.add("logs/pipeline.log", rotation="10 MB", retention="7 days", level="INFO")
logger.add("logs/rejected_records.log", filter=lambda r: "REJECTED" in r["message"], level="WARNING")