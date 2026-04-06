import streamlit as st
import json

def render_rule_form(rule_type, columns):
    """Return a params dict based on rule type."""
    params = {}

    if rule_type == "NOT_NULL":
        st.info("This rule checks that the selected column is NOT NULL.")
        return params  # no parameters

    elif rule_type == "VALUE_RANGE":
        params["min"] = st.number_input("Minimum Value", value=0)
        params["max"] = st.number_input("Maximum Value", value=100)
        return params

    elif rule_type == "STRING_LENGTH":
        params["min_len"] = st.number_input("Minimum Length", value=1)
        params["max_len"] = st.number_input("Maximum Length", value=100)
        return params

    elif rule_type == "PATTERN_MATCH":
        params["like"] = st.text_input("LIKE Pattern", "%@%")
        return params

    elif rule_type == "UNIQUENESS":
        params["key_columns"] = st.multiselect("Key Columns", columns)
        return params

    elif rule_type == "ALLOWED_VALUES":
        values = st.text_input("Allowed Values (comma-separated)")
        params["values"] = [v.strip() for v in values.split(",") if v.strip()]
        return params

    elif rule_type == "DATE_RANGE":
        params["min"] = st.text_input("Minimum Date (YYYY-MM-DD)")
        params["max"] = st.text_input("Maximum Date (YYYY-MM-DD)")
        return params

    elif rule_type == "CROSS_COLUMN":
        params["left"] = st.selectbox("Left Column", columns)
        params["op"] = st.selectbox("Operator", ["<=", "<", "=", ">=", ">"])
        params["right"] = st.selectbox("Right Column", columns)
        return params

    elif rule_type == "CONDITIONAL_REQUIRED":
        params["cond"] = st.text_input("Condition (SQL)", "[Status] = 'ACTIVE'")
        params["required_col"] = st.selectbox("Required Column", columns)
        return params

    elif rule_type == "DUPLICATE_ROWS":
        params["columns"] = st.multiselect("Columns to group by", columns)
        return params

    elif rule_type == "REFERENTIAL_INTEGRITY":
        st.info("Check that foreign key references exist in parent table.")
        params["fk_table"] = st.text_input("Parent Table (schema.table)")
        params["fk_column"] = st.text_input("Parent Table Primary Key Column")
        params["local_column"] = st.selectbox("Local Foreign Key Column", columns)
        return params

    elif rule_type == "ORPHANED_ROWS":
        st.info("Find rows where foreign key reference does not exist in parent table.")
        params["fk_table"] = st.text_input("Parent Table (schema.table)")
        params["fk_column"] = st.text_input("Parent Table Column")
        params["local_column"] = st.selectbox("Local Foreign Key Column", columns)
        return params

    elif rule_type == "COMPLETENESS":
        st.info("Check that required columns have values (not null, not empty).")
        params["required_columns"] = st.multiselect("Required Columns", columns)
        return params

    elif rule_type == "NEGATIVE_VALUES":
        st.info("Find rows where the selected column has negative values (< 0).")
        return params

    elif rule_type == "NON_NEGATIVE_VALUES":
        st.info("Check that the selected column contains only non-negative values (≥ 0).")
        return params

    elif rule_type == "POSITIVE_VALUES":
        st.info("Check that the selected column contains only positive values (> 0).")
        return params

    elif rule_type == "REGEX_MATCH":
        params["pattern"] = st.text_input("Regular Expression Pattern", "^[A-Z][A-Z0-9]*$")
        return params

    elif rule_type == "ROW_COUNT_MIN":
        params["min_rows"] = st.number_input("Minimum Row Count", value=1, min_value=1)
        return params

    elif rule_type == "CUSTOM_SQL_FILTER":
        mode = st.radio(
            "Custom SQL Mode",
            ["Filter (WHERE clause)", "Full SELECT query"],
            help="Use WHERE mode for table-scoped checks, or full SELECT for advanced custom validation.",
        )

        if mode == "Filter (WHERE clause)":
            params["where"] = st.text_area(
                "WHERE predicate",
                value="1 = 1",
                help="Write only the predicate part. Example: [Amount] > ? AND [Status] = ?",
            )
        else:
            params["sql"] = st.text_area(
                "Custom SELECT SQL",
                value="SELECT * FROM [dbo].[YourTable] WHERE 1 = 0",
                help="Only SELECT/CTE queries are allowed.",
                height=140,
            )

        params_json = st.text_input(
            "Query Parameters (JSON array)",
            value="[]",
            help="Example: [1000, \"ACTIVE\"]",
        )
        try:
            parsed = json.loads(params_json)
            if not isinstance(parsed, list):
                st.error("Parameters must be a JSON array, e.g. [1, \"A\"].")
                params["params"] = []
            else:
                params["params"] = parsed
        except json.JSONDecodeError:
            st.error("Invalid JSON parameters. Example: [1000, \"ACTIVE\"]")
            params["params"] = []

        return params

    else:
        st.warning("No parameters for this rule type.")
        return params

