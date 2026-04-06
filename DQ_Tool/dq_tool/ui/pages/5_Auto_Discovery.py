import json
import importlib.util
import math
import os
import sys
import tempfile
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

# Add sibling DataQuality project to import parser/graph/report modules.
DQ_ROOT = os.path.abspath(os.path.join(ROOT, "..", "DataQuality"))
if DQ_ROOT not in sys.path:
    sys.path.insert(0, DQ_ROOT)

from dq_tool.ui.components.connection_sidebar import connection_sidebar
from dq_engine.dq_engine import execute_session
from dq_engine.dq_logging import setup_logger


def _load_module_from_path(module_name: str, file_path: str):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module: {module_name} from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_discovery_dependencies():
    rel_path = os.path.join(DQ_ROOT, "relationship_graph.py")
    report_path = os.path.join(DQ_ROOT, "discovery_report.py")

    rel_mod = _load_module_from_path("dq_relationship_graph", rel_path)
    report_mod = _load_module_from_path("dq_discovery_report", report_path)

    return (
        rel_mod.build_relationship_graph,
        report_mod.build_discovery_report_payload,
        report_mod.write_discovery_reports,
    )


def _load_week6_schema_drift() -> List[Dict[str, Any]]:
    path = os.path.join(DQ_ROOT, "artifacts", "week6", "gap_analysis.json")
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload.get("schema_drift", []) or []
    except Exception:
        return []


def _score_confidence(join_count: int, evidence_files: int) -> float:
    join_score = min(0.6, 0.08 * math.log2(join_count + 1)) if join_count > 0 else 0.0
    evidence_score = min(0.2, 0.05 * max(0, evidence_files))
    return round(join_score + evidence_score, 3)


def _to_candidate_rows(parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
    joins = parsed.get("joins", [])
    grouped: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    for j in joins:
        left_table = str(getattr(j, "left_table", "") or "").lower()
        right_table = str(getattr(j, "right_table", "") or "").lower()
        left_col = str(getattr(j, "left_column", "") or "")
        right_col = str(getattr(j, "right_column", "") or "")

        if not left_table or not right_table or "." not in left_table or "." not in right_table:
            continue

        # Prefer orienting child -> parent when one side is *_ID and the other is ID.
        is_left_parentish = left_col.upper() == "ID"
        is_right_parentish = right_col.upper() == "ID"
        if is_left_parentish and not is_right_parentish:
            child_table, child_col = right_table, right_col
            parent_table, parent_col = left_table, left_col
        else:
            child_table, child_col = left_table, left_col
            parent_table, parent_col = right_table, right_col

        key = (child_table, child_col, parent_table, parent_col)
        row = grouped.get(key)
        if row is None:
            row = {
                "candidate_key": f"{child_table}.{child_col}->{parent_table}.{parent_col}",
                "child_table": child_table,
                "child_column": child_col,
                "parent_table": parent_table,
                "parent_column": parent_col,
                "join_count": 0,
                "join_types": set(),
                "object_names": set(),
                "evidence_files": set(),
            }
            grouped[key] = row

        row["join_count"] += 1
        row["join_types"].add(str(getattr(j, "join_type", "") or ""))
        row["object_names"].add(str(getattr(j, "object_name", "") or ""))
        row["evidence_files"].add(str(getattr(j, "source_file", "") or ""))

    out: List[Dict[str, Any]] = []
    for _, row in grouped.items():
        evidence_files = sorted([x for x in row["evidence_files"] if x])
        object_names = sorted([x for x in row["object_names"] if x])
        confidence = _score_confidence(row["join_count"], len(evidence_files))
        out.append(
            {
                "candidate_key": row["candidate_key"],
                "child_table": row["child_table"],
                "child_column": row["child_column"],
                "parent_table": row["parent_table"],
                "parent_column": row["parent_column"],
                "join_count": row["join_count"],
                "source": "JOIN_DISCOVERY",
                "confidence_score": confidence,
                "evidence_files": evidence_files,
                "evidence_objects": object_names,
                "join_types": sorted([x for x in row["join_types"] if x]),
            }
        )

    out.sort(key=lambda x: (float(x.get("confidence_score", 0.0)), int(x.get("join_count", 0))), reverse=True)
    return out


def _build_graph_html(candidates: List[Dict[str, Any]], max_edges: int = 120) -> str:
    try:
        from pyvis.network import Network
    except Exception:
        return ""

    net = Network(height="640px", width="100%", directed=True, bgcolor="#ffffff", font_color="#111111")
    net.barnes_hut(gravity=-18000, central_gravity=0.18, spring_length=180, spring_strength=0.02)

    added_nodes = set()
    for row in candidates[:max_edges]:
        child = row["child_table"]
        parent = row["parent_table"]
        if child not in added_nodes:
            net.add_node(child, label=child, color="#ffd166", shape="dot", size=14)
            added_nodes.add(child)
        if parent not in added_nodes:
            net.add_node(parent, label=parent, color="#06d6a0", shape="dot", size=14)
            added_nodes.add(parent)

        title = (
            f"{row['child_table']}.{row['child_column']} -> {row['parent_table']}.{row['parent_column']}<br>"
            f"join_count={row['join_count']}<br>confidence={row['confidence_score']}"
        )
        net.add_edge(
            child,
            parent,
            value=max(1, int(row.get("join_count", 1))),
            title=title,
            label=str(row.get("join_count", 0)),
            color="#118ab2",
            arrows="to",
        )

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
        net.write_html(tmp.name)
        html_path = tmp.name

    with open(html_path, "r", encoding="utf-8") as f:
        html_text = f.read()

    return html_text


def _load_session_decisions(candidates: List[Dict[str, Any]]) -> Dict[str, str]:
    state = st.session_state.setdefault("discovery_candidate_decisions", {})
    valid_keys = {c["candidate_key"] for c in candidates}
    to_remove = [k for k in state if k not in valid_keys]
    for k in to_remove:
        state.pop(k, None)
    return state


def _candidate_to_rule(candidate: Dict[str, Any]) -> Dict[str, Any]:
    child_schema, child_table = candidate["child_table"].split(".", 1)
    parent_schema, parent_table = candidate["parent_table"].split(".", 1)

    params = {
        "child_col": candidate["child_column"],
        "parent_schema": parent_schema,
        "parent_table": parent_table,
        "parent_col": candidate["parent_column"],
        "discovery_evidence": {
            "join_count": int(candidate.get("join_count", 0)),
            "confidence_score": float(candidate.get("confidence_score", 0.0)),
            "evidence_files": candidate.get("evidence_files", []),
            "evidence_objects": candidate.get("evidence_objects", []),
            "join_types": candidate.get("join_types", []),
            "source": candidate.get("source", "JOIN_DISCOVERY"),
        },
    }

    return {
        "rule_type": "REFERENTIAL_INTEGRITY",
        "target_schema": child_schema,
        "target_table": child_table,
        "target_column": candidate["child_column"],
        "parameters": params,
    }


def _rule_exists(pool, rule: Dict[str, Any]) -> bool:
    """Check if a rule with the same type/schema/table/column already exists."""
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM DQ.Rules
            WHERE rule_type = ? 
              AND target_schema = ? 
              AND target_table = ? 
              AND target_column = ?
            """,
            (
                rule["rule_type"],
                rule["target_schema"],
                rule["target_table"],
                rule["target_column"],
            ),
        )
        count = int(cur.fetchone()[0])
    return count > 0


def _insert_rule(pool, rule: Dict[str, Any]) -> int:
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO DQ.Rules (
                rule_type, target_schema, target_table, target_column,
                parameters_json, is_active, created_by
            )
            OUTPUT INSERTED.rule_id
            VALUES (?, ?, ?, ?, ?, 1, COALESCE(SUSER_SNAME(), SYSTEM_USER, 'UNKNOWN'))
            """,
            (
                rule["rule_type"],
                rule["target_schema"],
                rule["target_table"],
                rule["target_column"],
                json.dumps(rule["parameters"]),
            ),
        )
        rid = int(cur.fetchone()[0])
    return rid


st.title("🧭 Auto-Discovery")
connection_sidebar()

try:
    build_relationship_graph, build_discovery_report_payload, write_discovery_reports = _load_discovery_dependencies()
except Exception as exc:
    st.error(
        "Could not load Week 5/6 discovery modules from DataQuality. "
        f"Details: {exc}"
    )
    st.stop()

left, right = st.columns([2, 1])
with left:
    default_repo = st.session_state.get("sql_root_path", ROOT)
    repo_path = st.text_input("SQL Repository Path", value=default_repo)
with right:
    min_join_count = st.number_input("Min JOIN Count", min_value=1, max_value=1000, value=2, step=1)

run_parse = st.button("Run Discovery Parser", type="primary")

if run_parse:
    path = os.path.abspath(repo_path)
    if not os.path.isdir(path):
        st.error(f"Path does not exist: {path}")
        st.stop()

    with st.spinner("Parsing SQL tree and building relationship graphs..."):
        result = build_relationship_graph(path)
        candidates = _to_candidate_rows(result["parsed"])
        candidates = [c for c in candidates if int(c.get("join_count", 0)) >= int(min_join_count)]

    st.session_state["discovery_result"] = result
    st.session_state["discovery_candidates"] = candidates
    st.session_state["discovery_repo_path"] = path
    st.success(
        "Discovery completed. "
        f"SQL files={result['summary']['sql_files']}, tables={result['summary']['tables']}, joins={result['summary']['joins']}, "
        f"candidates={len(candidates)}"
    )

if "discovery_result" not in st.session_state:
    st.info("Run the parser first to populate candidates and the relationship graph.")
    st.stop()

result = st.session_state["discovery_result"]
candidates = st.session_state.get("discovery_candidates", [])

summary = result.get("summary", {})
sm1, sm2, sm3, sm4, sm5 = st.columns(5)
sm1.metric("SQL Files", int(summary.get("sql_files", 0)))
sm2.metric("Tables", int(summary.get("tables", 0)))
sm3.metric("JOINs", int(summary.get("joins", 0)))
sm4.metric("FK Edges", int(summary.get("fk_edges", 0)))
sm5.metric("Candidates", len(candidates))

st.subheader("Relationship Graph")
graph_html = _build_graph_html(candidates, max_edges=140)
if graph_html:
    components.html(graph_html, height=680, scrolling=True)
else:
    st.warning("pyvis is not available in this environment. Install it with: pip install pyvis")
    fallback_rows = [
        {
            "child": c["child_table"],
            "parent": c["parent_table"],
            "join_count": c["join_count"],
            "confidence": c["confidence_score"],
        }
        for c in candidates[:200]
    ]
    st.dataframe(pd.DataFrame(fallback_rows), use_container_width=True)

st.subheader("Candidate Rules Review")
if not candidates:
    st.warning("No candidates found with current filters.")
    st.stop()

decisions = _load_session_decisions(candidates)

rows = []
for c in candidates:
    key = c["candidate_key"]
    d = decisions.get(key, "pending")
    rows.append(
        {
            "candidate_key": key,
            "source": c.get("source", "JOIN_DISCOVERY"),
            "target": f"{c['child_table']}.{c['child_column']}",
            "reference": f"{c['parent_table']}.{c['parent_column']}",
            "join_count": int(c.get("join_count", 0)),
            "confidence": float(c.get("confidence_score", 0.0)),
            "evidence_files": len(c.get("evidence_files", [])),
            "evidence_objects": len(c.get("evidence_objects", [])),
            "accept": d == "accepted",
            "reject": d == "rejected",
        }
    )

review_df = pd.DataFrame(rows)
edited = st.data_editor(
    review_df,
    use_container_width=True,
    hide_index=True,
    disabled=["candidate_key", "source", "target", "reference", "join_count", "confidence", "evidence_files", "evidence_objects"],
    key="discovery_review_editor",
)

if st.button("Apply Review Decisions"):
    for _, row in edited.iterrows():
        key = str(row["candidate_key"])
        accept = bool(row["accept"])
        reject = bool(row["reject"])
        if accept and reject:
            continue
        if accept:
            decisions[key] = "accepted"
        elif reject:
            decisions[key] = "rejected"
        else:
            decisions[key] = "pending"
    st.success("Review decisions updated.")

accepted = [c for c in candidates if decisions.get(c["candidate_key"]) == "accepted"]
rejected = [c for c in candidates if decisions.get(c["candidate_key"]) == "rejected"]
pending = [c for c in candidates if decisions.get(c["candidate_key"], "pending") == "pending"]

c1, c2, c3 = st.columns(3)
c1.metric("Accepted", len(accepted))
c2.metric("Rejected", len(rejected))
c3.metric("Pending", len(pending))

with st.expander("Evidence by candidate", expanded=False):
    for c in accepted[:20] + pending[:10]:
        st.markdown(f"**{c['candidate_key']}**")
        st.caption("Objects: " + ", ".join(c.get("evidence_objects", [])[:8]))
        for fp in c.get("evidence_files", [])[:12]:
            st.code(fp)

st.subheader("Accept -> Create DQ Rules")
has_pool = "pool" in st.session_state
if not has_pool:
    st.warning("Connect to database from the sidebar to persist accepted rules into DQ.Rules.")

run_after_insert = st.checkbox("Run accepted rules immediately (Phase 1 engine)", value=False, disabled=not has_pool)

if st.button("Push Accepted to DQ.Rules", disabled=(not has_pool or not accepted)):
    pool = st.session_state.pool
    inserted_ids: List[int] = []
    skipped_duplicates: List[str] = []
    executable_rules: List[Dict[str, Any]] = []
    errors: List[str] = []

    for c in accepted:
        try:
            rule = _candidate_to_rule(c)
            
            # Check if rule already exists
            if _rule_exists(pool, rule):
                skipped_duplicates.append(
                    f"{rule['target_schema']}.{rule['target_table']}.{rule['target_column']} "
                    f"({rule['rule_type']})"
                )
                continue
            
            rid = _insert_rule(pool, rule)
            inserted_ids.append(rid)
            executable_rules.append(
                {
                    "rule_id": rid,
                    "rule_type": rule["rule_type"],
                    "target_schema": rule["target_schema"],
                    "target_table": rule["target_table"],
                    "target_column": rule["target_column"],
                    "parameters": rule["parameters"],
                }
            )
        except Exception as exc:
            errors.append(f"{c['candidate_key']}: {exc}")

    # Display results
    col1, col2, col3 = st.columns(3)
    
    if inserted_ids:
        with col1:
            st.success(f"✅ Inserted {len(inserted_ids)} new rules")
            if len(inserted_ids) <= 10:
                st.code(f"Rule IDs: {inserted_ids}", language="text")
            else:
                st.code(f"Rule IDs: {inserted_ids[:10]} + {len(inserted_ids) - 10} more", language="text")
    
    if skipped_duplicates:
        with col2:
            st.warning(f"⏭️ Skipped {len(skipped_duplicates)} duplicates")
            with st.expander("View skipped rules"):
                for d in skipped_duplicates[:20]:
                    st.text(f"• {d}")
                if len(skipped_duplicates) > 20:
                    st.text(f"... and {len(skipped_duplicates) - 20} more")

    if errors:
        with col3:
            st.error(f"❌ {len(errors)} errors during insert")
            with st.expander("View errors"):
                for e in errors[:20]:
                    st.code(e)
                if len(errors) > 20:
                    st.text(f"... and {len(errors) - 20} more")

    if run_after_insert and executable_rules:
        logger = setup_logger("dq_ui.log")
        session_result = execute_session(
            pool,
            executable_rules,
            executed_by="AutoDiscovery",
            max_workers=min(5, max(1, len(executable_rules))),
            logger=logger,
        )
        st.success(f"Execution session completed: session_id={session_result.session_id}")

st.subheader("Discovery Report Generator")
report_dir = st.text_input(
    "Report output directory",
    value=os.path.join(DQ_ROOT, "artifacts", "week7"),
)
schema_drift_rows = _load_week6_schema_drift()
#st.caption(f"Week 6 schema drift rows available for report: {len(schema_drift_rows)}")

if st.button("Generate Markdown + HTML Report"):
    if build_discovery_report_payload is None or write_discovery_reports is None:
        st.error("Report generator module is unavailable.")
        st.stop()

    payload = build_discovery_report_payload(
        project_root=st.session_state.get("discovery_repo_path", repo_path),
        parse_summary={
            "sql_files": summary.get("sql_files", 0),
            "tables": summary.get("tables", 0),
            "joins": summary.get("joins", 0),
        },
        parsed_tables=result["parsed"].get("tables", {}),
        fk_edge_count=int(summary.get("fk_edges", 0)),
        join_edge_count=int(summary.get("join_edges", 0)),
        inferred_relationships=result.get("inferred_relationships", []),
        candidates=candidates,
        schema_drift_rows=schema_drift_rows,
        accepted_count=len(accepted),
        rejected_count=len(rejected),
    )

    paths = write_discovery_reports(report_dir, payload, basename="discovery_report")
    st.success("Report generated successfully.")
    st.code(json.dumps(paths, indent=2))


