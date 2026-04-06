
# dq_engine/tests/unit/test_utils.py
#helping tool; it cleans string SQL for comparison by collapsing whitespace and trimming.
def norm_sql(s: str) -> str:
    """Collapse whitespace for comparison."""
    if s is None:
        return ""
    return " ".join(s.split()).strip()