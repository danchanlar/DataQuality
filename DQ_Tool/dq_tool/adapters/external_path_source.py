from pathlib import Path

class ExternalPathSource:
    def __init__(self, root_path: str):
        self.root = Path(root_path)

    def list_sql_files(self):
        if not self.root.exists():
            return []
        return self.root.rglob('*.sql')

