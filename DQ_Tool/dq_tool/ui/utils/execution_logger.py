"""
Execution logging for Data Quality rules.
Logs to a file with timestamps for real-time monitoring.
"""

import os
import logging
from datetime import datetime
from pathlib import Path

# Ensure logs directory exists
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

EXECUTION_LOG_FILE = LOGS_DIR / "execution.log"


def get_execution_logger(name="execution"):
    """Get or create a logger for execution events."""
    logger = logging.getLogger(name)
    
    # Only add handler if not already present
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        # File handler - write to execution.log
        fh = logging.FileHandler(EXECUTION_LOG_FILE, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        
        # Formatter with timestamp
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh.setFormatter(formatter)
        
        logger.addHandler(fh)
    
    return logger


def log_execution_start(rule_id, rule_type, target_table, max_workers, pool_size):
    """Log the start of an execution session."""
    logger = get_execution_logger()
    logger.info(f"═" * 80)
    logger.info(f"EXECUTION STARTED")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Rule ID: {rule_id} | Type: {rule_type} | Table: {target_table}")
    logger.info(f"Max Workers: {max_workers} | Pool Size: {pool_size}")
    logger.info(f"─" * 80)


def log_rule_execution(rule_id, target_table, status, message=""):
    """Log individual rule execution."""
    logger = get_execution_logger()
    rule_id_str = f"{rule_id:4d}" if isinstance(rule_id, int) else str(rule_id).rjust(4)
    msg = f"Rule {rule_id_str} | Table: {target_table:40s} | Status: {status}"
    if message:
        msg += f" | {message}"
    logger.info(msg)


def log_execution_end(total_rules, violations_found, duration_seconds):
    """Log the end of an execution session."""
    logger = get_execution_logger()
    logger.info(f"─" * 80)
    logger.info(f"EXECUTION COMPLETED")
    logger.info(f"Total Rules: {total_rules} | Violations Found: {violations_found}")
    logger.info(f"Duration: {duration_seconds:.2f} seconds")
    logger.info(f"═" * 80)
    logger.info("")


def log_error(rule_id, target_table, error_message):
    """Log an error during rule execution."""
    logger = get_execution_logger()
    logger.error(f"Rule {rule_id} | Table: {target_table} | ERROR: {error_message}")


def get_execution_log_content(last_n_lines=100):
    """Read the last N lines from execution log file (safe on Windows)."""
    if not EXECUTION_LOG_FILE.exists():
        return "No execution log file found yet."
    
    try:
        # Use read-only share mode compatible with Windows locked files
        with open(EXECUTION_LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        tail = lines[-last_n_lines:] if len(lines) > last_n_lines else lines
        return "".join(tail)
    except PermissionError:
        return "[Log is temporarily locked by the execution process — refresh in a moment]"
    except Exception as e:
        return f"Error reading log file: {e}"


def clear_execution_log():
    """Clear the execution log file safely on Windows.
    
    Does NOT delete the file (fails on Windows when FileHandler keeps it open).
    Truncates in place instead.
    """
    logger = logging.getLogger("execution")
    # Flush all handlers before truncating
    for h in logger.handlers:
        try:
            h.flush()
        except Exception:
            pass

    try:
        with open(EXECUTION_LOG_FILE, "w", encoding="utf-8"):
            pass  # truncate to zero bytes
    except FileNotFoundError:
        EXECUTION_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(EXECUTION_LOG_FILE, "w", encoding="utf-8"):
            pass


