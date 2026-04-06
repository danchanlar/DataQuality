from typing import Protocol, Iterable
from pathlib import Path

class SqlSourceAdapter(Protocol):
    def list_sql_files(self) -> Iterable[Path]:
        ...

