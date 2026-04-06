

"""
Logging utilities for the DQ engine.
- setup_logger(log_file): returns a configured Python logger (file + console)
- safe_json(obj): JSON string with safe defaults
- build_rule_summary_dict(...): normalized dict for result_summary logging
"""
import json
import logging
from logging import Logger
from typing import Any, Dict

def setup_logger(log_file: str = 'dq_engine.log', level: int = logging.INFO) -> Logger:
    logger = logging.getLogger('dq_engine')
    logger.setLevel(level)

    if not logger.handlers:
        fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger

def safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        try:
            return json.dumps(str(obj))
        except Exception:
            return '{}'

def build_rule_summary_dict(sql: str, params, row_count_checked: int, violations_count: int, sample_violations):
    return {
        'sql': sql,
        'params': params,
        'row_count_checked': row_count_checked,
        'violations_count': violations_count,
        'sample_violations': sample_violations,
    }