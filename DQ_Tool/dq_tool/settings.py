from dataclasses import dataclass
import os

@dataclass
class SqlSourceSettings:
    mode: str = os.getenv('DQ_SQL_SOURCE_MODE', 'external_path')
    root_path: str = os.getenv('DQ_SQL_ROOT_PATH', '')

