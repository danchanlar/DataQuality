"""
Thread-safe execution progress state.

Used to share live progress between the background execution thread
and the Streamlit main thread (which cannot share session_state safely).
"""

import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional, Any


@dataclass
class RuleProgress:
    rule_id: int
    target: str
    rule_type: str
    status: str          # "running" | "completed" | "failed"
    started_at: float    # time.monotonic()
    finished_at: Optional[float] = None
    error: Optional[str] = None

    @property
    def duration(self) -> Optional[float]:
        if self.finished_at is not None:
            return self.finished_at - self.started_at
        return time.monotonic() - self.started_at  # still running


class ExecutionProgress:
    """
    Thread-safe progress tracker for a single execution session.
    Written to by background thread, read by Streamlit main thread.
    """

    def __init__(self, total: int):
        self._lock = threading.Lock()
        self.total = total
        self.completed = 0
        self.failed = 0
        self.done = False
        self.result = None          # SessionResult when done
        self.error: Optional[str] = None  # top-level error if session crashed
        self.started_at = time.monotonic()
        self.finished_at: Optional[float] = None
        self._rules: List[RuleProgress] = []

    # ------------------------------------------------------------------ writes (background thread)

    def rule_done(self, execution_result, exception):
        """Called by on_rule_complete callback from worker thread."""
        with self._lock:
            if exception is not None:
                self.failed += 1
                self.completed += 1
            else:
                status = "completed" if execution_result.status == "Succeeded" else "failed"
                if status == "failed":
                    self.failed += 1
                self.completed += 1

    def mark_done(self, result=None, error: Optional[str] = None):
        with self._lock:
            self.done = True
            self.finished_at = time.monotonic()
            self.result = result
            self.error = error

    # ------------------------------------------------------------------ reads (main thread)

    def snapshot(self):
        """Return a consistent read-only snapshot."""
        with self._lock:
            return {
                "total": self.total,
                "completed": self.completed,
                "failed": self.failed,
                "done": self.done,
                "error": self.error,
                "result": self.result,
                "elapsed": (
                    self.finished_at or time.monotonic()
                ) - self.started_at,
            }


