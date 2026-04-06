
"""
SQL generator functions for 19 rule types.

Each generator returns:
    (sql_text: str, params: list)
where sql_text uses '?' placeholders for pyodbc, and params is an ordered list.

Rule parameter contract:
- We expect a Python dict 'rule' with keys mirroring DQ.Rules:
  {
    "rule_id": int,
    "rule_type": str,
    "target_schema": str,
    "target_table": str,
    "target_column": Optional[str],
    "parameters": dict   # parsed from parameters_json
  }

Parameters per rule (examples):
- UNIQUENESS:       parameters = {"key_columns": ["ColA", "ColB"]}  # REQUIRED
- NOT_NULL:         parameters = {}  (uses target_column)
- VALUE_RANGE:      parameters = {"min": 0, "max": 100}              # one or both
- PATTERN_MATCH:    parameters = {"like": "%@%.%", "case_sensitive": False}
- STRING_LENGTH:    parameters = {"min_len": 5, "max_len": 50}
- ALLOWED_VALUES:   parameters = {"values": ["A","B","C"], "case_sensitive": False}
- DATE_RANGE:       parameters = {"min": "2020-01-01", "max": "2025-12-31"}
- CROSS_COLUMN:     parameters = {"op": "<=", "left": "StartDate", "right": "EndDate"}
- CONDITIONAL_REQ:  parameters = {"cond": "[Status] = 'ACTIVE'", "required_col": "EndDate"}

Notes:
- All generators focus on producing the "violations query": rows that violate.
- Caller is responsible for turning result rows into entries for DQ.RuleViolations.
"""

from typing import Dict, Tuple, List, Optional
import re

ALLOWED_RULE_TYPES = [
    "UNIQUENESS",
    "NOT_NULL",
    "VALUE_RANGE",
    "PATTERN_MATCH",
    "DUPLICATE_ROWS",
    "STRING_LENGTH",
    "ALLOWED_VALUES",
    "DATE_RANGE",
    "CROSS_COLUMN",
    "CONDITIONAL_REQUIRED",
    # SQL-native / relational, they are implented but not yet exposed in UI (Page 1) or tests
    "REFERENTIAL_INTEGRITY",
    "ORPHANED_ROWS",
    "COMPLETENESS",
    "NEGATIVE_VALUES",
    "NON_NEGATIVE_VALUES",
    "POSITIVE_VALUES",
    "CUSTOM_SQL_FILTER",
    "REGEX_MATCH",  # via PATINDEX if simple; true regex needs CLR or LIKE patterns workaround
    "ROW_COUNT_MIN",  # e.g., expect at least N rows
]

def _q_ident(name: str) -> str:
    """Quote identifier with brackets, basic sanitization."""
    if name is None:
        return ""
    # deny dangerous chars
    safe = name.replace("]", "]]")
    return f"[{safe}]"

def _full_table(schema: str, table: str) -> str:
    return f"{_q_ident(schema)}.{_q_ident(table)}"

def _column_list(cols: List[str]) -> str:
    return ", ".join(_q_ident(c) for c in cols)

def generate_sql(rule: Dict) -> Tuple[str, List]:
    """Dispatcher."""
    rtype = rule["rule_type"].upper()
    if rtype == "UNIQUENESS":
        return generate_uniqueness_query(rule)
    elif rtype == "NOT_NULL":
        return generate_not_null_query(rule)
    elif rtype == "VALUE_RANGE":
        return generate_value_range_query(rule)
    elif rtype == "PATTERN_MATCH":
        return generate_pattern_match_query(rule)
    elif rtype == "DUPLICATE_ROWS":
        return generate_duplicate_rows_query(rule)
    elif rtype == "STRING_LENGTH":
        return generate_string_length_query(rule)
    elif rtype == "ALLOWED_VALUES":
        return generate_allowed_values_query(rule)
    elif rtype == "DATE_RANGE":
        return generate_date_range_query(rule)
    elif rtype == "CROSS_COLUMN":
        return generate_cross_column_query(rule)
    elif rtype == "CONDITIONAL_REQUIRED":
        return generate_conditional_required_query(rule)
    elif rtype == "REFERENTIAL_INTEGRITY":
        return generate_referential_integrity_query(rule)
    elif rtype == "ORPHANED_ROWS":
        return generate_orphaned_rows_query(rule)
    elif rtype == "COMPLETENESS":
        return generate_completeness_query(rule)
    elif rtype == "NEGATIVE_VALUES":
        return generate_negative_values_query(rule)
    elif rtype == "NON_NEGATIVE_VALUES":
        return generate_non_negative_values_query(rule)
    elif rtype == "POSITIVE_VALUES":
        return generate_positive_values_query(rule)
    elif rtype == "CUSTOM_SQL_FILTER":
        return generate_custom_sql_filter_query(rule)
    elif rtype == "REGEX_MATCH":
        return generate_regex_match_query(rule)
    elif rtype == "ROW_COUNT_MIN":
        return generate_row_count_min_query(rule)
    else:
        raise ValueError(f"Unsupported rule_type: {rtype}")

# ----------------------------
# Implemented: 5 core rules
# ----------------------------

def generate_uniqueness_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = key combinations that appear more than once.
    parameters: {"key_columns": ["ColA", "ColB"]}
    Returns rows with the duplicate key values and their counts.
    """
    params = rule.get("parameters") or {}
    keys = params.get("key_columns")
    if not keys or not isinstance(keys, list):
        raise ValueError("UNIQUENESS requires parameters.key_columns as a list")

    table = _full_table(rule["target_schema"], rule["target_table"])
    grouping = _column_list(keys)
    sql = f"""
        SELECT {grouping}, COUNT(*) AS dup_count
        FROM {table}
        GROUP BY {grouping}
        HAVING COUNT(*) > 1
    """
    return sql, []

def generate_not_null_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = rows where target_column IS NULL.
    """
    col = rule.get("target_column")
    if not col:
        raise ValueError("NOT_NULL requires target_column")
    table = _full_table(rule["target_schema"], rule["target_table"])
    sql = f"""
        SELECT *
        FROM {table}
        WHERE {_q_ident(col)} IS NULL
    """
    return sql, []

def generate_value_range_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = rows where col < min OR col > max (if provided).
    parameters: {"min": x?, "max": y?}
    """
    col = rule.get("target_column")
    if not col:
        raise ValueError("VALUE_RANGE requires target_column")
    params_obj = rule.get("parameters") or {}
    has_min = "min" in params_obj and params_obj["min"] is not None
    has_max = "max" in params_obj and params_obj["max"] is not None

    if not (has_min or has_max):
        raise ValueError("VALUE_RANGE requires at least one of parameters.min or parameters.max")

    table = _full_table(rule["target_schema"], rule["target_table"])
    conditions = []
    params = []
    if has_min:
        conditions.append(f"{_q_ident(col)} < ?")
        params.append(params_obj["min"])
    if has_max:
        conditions.append(f"{_q_ident(col)} > ?")
        params.append(params_obj["max"])

    where = " OR ".join(conditions)
    sql = f"SELECT * FROM {table} WHERE {where}"
    return sql, params

def generate_pattern_match_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = rows where column NOT LIKE pattern (case sensitivity depends on collation).
    parameters: {"like": "%@%.%", "case_sensitive": False}
    """
    col = rule.get("target_column")
    if not col:
        raise ValueError("PATTERN_MATCH requires target_column")
    params_obj = rule.get("parameters") or {}
    like = params_obj.get("like")
    if like is None:
        raise ValueError("PATTERN_MATCH requires parameters.like")
    table = _full_table(rule["target_schema"], rule["target_table"])
    sql = f"""
        SELECT *
        FROM {table}
        WHERE {_q_ident(col)} NOT LIKE ?
    """
    return sql, [like]

def generate_string_length_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = LEN(col) < min_len OR LEN(col) > max_len (if provided)
    parameters: {"min_len": x?, "max_len": y?}
    """
    col = rule.get("target_column")
    if not col:
        raise ValueError("STRING_LENGTH requires target_column")
    params_obj = rule.get("parameters") or {}
    has_min = "min_len" in params_obj and params_obj["min_len"] is not None
    has_max = "max_len" in params_obj and params_obj["max_len"] is not None
    if not (has_min or has_max):
        raise ValueError("STRING_LENGTH requires at least one of parameters.min_len or parameters.max_len")

    table = _full_table(rule["target_schema"], rule["target_table"])
    conditions = []
    params = []
    if has_min:
        conditions.append("LEN({_col}) < ?".format(_col=_q_ident(col)))
        params.append(int(params_obj["min_len"]))
    if has_max:
        conditions.append("LEN({_col}) > ?".format(_col=_q_ident(col)))
        params.append(int(params_obj["max_len"]))
    where = " OR ".join(conditions)
    sql = f"SELECT * FROM {table} WHERE {where}"
    return sql, params

# ----------------------------
# Stubs for other rule types
# ----------------------------

def generate_duplicate_rows_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = fully duplicated rows across supplied columns
    parameters: {"columns": ["ColA","ColB", ...]}  # REQUIRED
    """
    params = rule.get("parameters") or {}
    cols = params.get("columns")
    if not cols or not isinstance(cols, list):
        raise ValueError("DUPLICATE_ROWS requires parameters.columns as a list")
    table = _full_table(rule["target_schema"], rule["target_table"])
    grouping = _column_list(cols)
    sql = f"""
        SELECT {grouping}, COUNT(*) AS dup_count
        FROM {table}
        GROUP BY {grouping}
        HAVING COUNT(*) > 1
    """
    return sql, []

def generate_allowed_values_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = values NOT IN (list)
    parameters: {"values": [...], "case_sensitive": False}
    """
    col = rule.get("target_column")
    if not col:
        raise ValueError("ALLOWED_VALUES requires target_column")
    params_obj = rule.get("parameters") or {}
    values = params_obj.get("values")
    if not values or not isinstance(values, list):
        raise ValueError("ALLOWED_VALUES requires parameters.values list")
    table = _full_table(rule["target_schema"], rule["target_table"])
    placeholders = ", ".join(["?"] * len(values))
    sql = f"""
        SELECT *
        FROM {table}
        WHERE {_q_ident(col)} NOT IN ({placeholders})
           OR {_q_ident(col)} IS NULL
    """
    return sql, values

def generate_date_range_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = dates outside [min, max]
    parameters: {"min": "YYYY-MM-DD", "max": "YYYY-MM-DD"} (one or both)
    """
    col = rule.get("target_column")
    if not col:
        raise ValueError("DATE_RANGE requires target_column")
    params_obj = rule.get("parameters") or {}
    has_min = "min" in params_obj and params_obj["min"] is not None
    has_max = "max" in params_obj and params_obj["max"] is not None
    if not (has_min or has_max):
        raise ValueError("DATE_RANGE requires at least one of parameters.min or parameters.max")
    table = _full_table(rule["target_schema"], rule["target_table"])
    conditions = []
    params = []
    if has_min:
        conditions.append(f"{_q_ident(col)} < ?")
        params.append(params_obj["min"])
    if has_max:
        conditions.append(f"{_q_ident(col)} > ?")
        params.append(params_obj["max"])
    where = " OR ".join(conditions)
    sql = f"SELECT * FROM {table} WHERE {where}"
    return sql, params

def generate_cross_column_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = NOT (left op right)
    parameters: {"op": "<=|<|>=|>|=|<>", "left": "ColA", "right": "ColB"}
    """
    p = rule.get("parameters") or {}
    op = p.get("op")
    left = p.get("left")
    right = p.get("right")
    if not op or not left or not right:
        raise ValueError("CROSS_COLUMN requires parameters.op, parameters.left, parameters.right")
    table = _full_table(rule["target_schema"], rule["target_table"])
    sql = f"""
        SELECT *
        FROM {table}
        WHERE NOT ({_q_ident(left)} {op} {_q_ident(right)})
    """
    return sql, []

def generate_conditional_required_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = when condition holds, required_col IS NULL
    parameters: {"cond": "<SQL condition>", "required_col": "ColB"}
    """
    p = rule.get("parameters") or {}
    cond = p.get("cond")
    req = p.get("required_col")
    if not cond or not req:
        raise ValueError("CONDITIONAL_REQUIRED requires parameters.cond and parameters.required_col")
    table = _full_table(rule["target_schema"], rule["target_table"])
    sql = f"""
        SELECT *
        FROM {table}
        WHERE ({cond}) AND {_q_ident(req)} IS NULL
    """
    return sql, []

def generate_referential_integrity_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = child rows referencing missing parent
    parameters: {"child_col": "...", "parent_schema": "...", "parent_table": "...", "parent_col": "..."}
    """
    p = rule.get("parameters") or {}
    child_col = rule.get("target_column") or p.get("child_col")
    ps = p.get("parent_schema")
    pt = p.get("parent_table")
    pc = p.get("parent_col")
    if not child_col or not ps or not pt or not pc:
        raise ValueError("REFERENTIAL_INTEGRITY requires child_col/target_column and parent_schema/table/col")
    child = _full_table(rule["target_schema"], rule["target_table"])
    parent = _full_table(ps, pt)
    sql = f"""
        SELECT c.*
        FROM {child} AS c
        LEFT JOIN {parent} AS p
          ON c.{_q_ident(child_col)} = p.{_q_ident(pc)}
        WHERE p.{_q_ident(pc)} IS NULL
          AND c.{_q_ident(child_col)} IS NOT NULL
    """
    return sql, []

def generate_orphaned_rows_query(rule: Dict) -> Tuple[str, List]:
    """
    Alias of referential_integrity in practice (or scenario-specific)
    """
    return generate_referential_integrity_query(rule)

def generate_completeness_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = rows missing required fields
    parameters: {"required_columns": ["A","B","C"]}
    """
    p = rule.get("parameters") or {}
    cols = p.get("required_columns")
    if not cols or not isinstance(cols, list):
        raise ValueError("COMPLETENESS requires parameters.required_columns")
    table = _full_table(rule["target_schema"], rule["target_table"])
    conditions = [f"{_q_ident(c)} IS NULL" for c in cols]
    where = " OR ".join(conditions)
    sql = f"SELECT * FROM {table} WHERE {where}"
    return sql, []

def generate_negative_values_query(rule: Dict) -> Tuple[str, List]:
    col = rule.get("target_column")
    if not col:
        raise ValueError("NEGATIVE_VALUES requires target_column")
    table = _full_table(rule["target_schema"], rule["target_table"])
    sql = f"SELECT * FROM {table} WHERE {_q_ident(col)} < 0"
    return sql, []

def generate_non_negative_values_query(rule: Dict) -> Tuple[str, List]:
    col = rule.get("target_column")
    if not col:
        raise ValueError("NON_NEGATIVE_VALUES requires target_column")
    table = _full_table(rule["target_schema"], rule["target_table"])
    sql = f"SELECT * FROM {table} WHERE {_q_ident(col)} < 0"
    # NOTE: NON_NEGATIVE violation = values < 0
    return sql, []

def generate_positive_values_query(rule: Dict) -> Tuple[str, List]:
    col = rule.get("target_column")
    if not col:
        raise ValueError("POSITIVE_VALUES requires target_column")
    table = _full_table(rule["target_schema"], rule["target_table"])
    sql = f"SELECT * FROM {table} WHERE {_q_ident(col)} <= 0"
    return sql, []

def generate_custom_sql_filter_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations = rows matching custom SQL criteria.
    parameters:
      - WHERE mode: {"where": "<SQL predicate with ? placeholders>", "params": [..]}
      - Full SQL mode: {"sql": "<SELECT ...>", "params": [..]}
    """
    p = rule.get("parameters") or {}
    custom_sql = p.get("sql")
    where = p.get("where")
    params = p.get("params", [])

    if custom_sql:
        sql = str(custom_sql).strip()
        if not sql:
            raise ValueError("CUSTOM_SQL_FILTER parameters.sql cannot be empty")

        # Keep this rule read-only: only SELECT/CTE statements are allowed.
        sql_wo_trailing = sql.rstrip().rstrip(";").strip()
        lowered = sql_wo_trailing.lower()
        if not (lowered.startswith("select") or lowered.startswith("with")):
            raise ValueError("CUSTOM_SQL_FILTER parameters.sql must start with SELECT or WITH")
        if ";" in sql_wo_trailing:
            raise ValueError("CUSTOM_SQL_FILTER parameters.sql must contain a single statement")
        if re.search(r"\b(insert|update|delete|merge|drop|alter|create|truncate|exec|execute)\b", lowered):
            raise ValueError("CUSTOM_SQL_FILTER parameters.sql must be read-only")

        return sql_wo_trailing, list(params)

    if not where:
        raise ValueError("CUSTOM_SQL_FILTER requires parameters.where or parameters.sql")

    table = _full_table(rule["target_schema"], rule["target_table"])
    sql = f"SELECT * FROM {table} WHERE {where}"
    return sql, list(params)

def generate_regex_match_query(rule: Dict) -> Tuple[str, List]:
    """
    Approximate regex via LIKE/PATINDEX for SQL Server (true regex not native).
    parameters: {"patindex": "%[0-9][0-9][0-9]%", "negate": True/False}
    """
    col = rule.get("target_column")
    if not col:
        raise ValueError("REGEX_MATCH requires target_column")
    p = rule.get("parameters") or {}
    pat = p.get("patindex")
    negate = bool(p.get("negate", False))
    if not pat:
        raise ValueError("REGEX_MATCH requires parameters.patindex")
    table = _full_table(rule["target_schema"], rule["target_table"])
    if negate:
        sql = f"SELECT * FROM {table} WHERE PATINDEX(?, {_q_ident(col)}) = 0"
    else:
        sql = f"SELECT * FROM {table} WHERE PATINDEX(?, {_q_ident(col)}) = 0"  # match failure => violation
    return sql, [pat]

def generate_row_count_min_query(rule: Dict) -> Tuple[str, List]:
    """
    Violations when row count < min_rows (return a single row describing the failure)
    parameters: {"min_rows": 100}
    """
    p = rule.get("parameters") or {}
    min_rows = p.get("min_rows")
    if min_rows is None:
        raise ValueError("ROW_COUNT_MIN requires parameters.min_rows")
    table = _full_table(rule["target_schema"], rule["target_table"])
    # Return a synthetic row with details if count < min_rows
    sql = f"""
        WITH cte AS (
            SELECT COUNT(*) AS cnt FROM {table}
        )
        SELECT *
        FROM cte
        WHERE cnt < ?
    """
    return sql, [int(min_rows)]