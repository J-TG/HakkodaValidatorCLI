"""
Decisions page

Log and validate decisions around data architecture and governance.

Features:
- Decision categories: Ingestion, Modelling, Info_Mart, Governance
- Decision metadata: name, description, pseudo code, assignee, dates
- Validation tests: SQL that should return 0 results when passing
- Test execution and monitoring
"""

import copy
import streamlit as st
from datetime import date, datetime
from typing import Dict, List, Any, Optional
from common.layout import init_page, get_runtime_mode, get_db_connection
from common.db import run_query, get_snowflake_table_prefix


# =============================================================================
# Decision Categories
# =============================================================================

DECISION_CATEGORIES = [
    "Ingestion",
    "Modelling", 
    "Info_Mart",
    "Governance",
    "Security",
    "Performance",
    "Other",
]

ALERT_LEVELS = ["INFO", "WARN", "ERROR"]
DEFAULT_DECISION_ALERT_LEVEL = "ERROR"
DEFAULT_EXCEPTION_ALERT_LEVEL = "WARN"


# =============================================================================
# Sample Decisions (Demo Data)
# =============================================================================

SAMPLE_DECISIONS: List[Dict[str, Any]] = [
    {
        "id": 1,
        "category": "Ingestion",
        "name": "No NULL values in primary keys",
        "description": "All ingested tables must have non-NULL primary key columns to ensure data integrity and proper joins.",
        "pseudo_code": "SELECT * FROM table WHERE pk_column IS NULL → Should return 0 rows",
        "assigned_to": "Data Engineering Team",
        "date_decided": date(2025, 11, 15),
        "date_enforced": date(2025, 12, 1),
        "test_sql": "SELECT COUNT(*) as violations FROM {schema}.TESTTABLE WHERE CC_CALL_CENTER_SK IS NULL;",
        "status": "Active",
        "severity": "ERROR",
        "exceptions": [
            {
                "id": 1,
                "name": "Legacy call centers still onboarding",
                "description": "Allow a few call centers without open dates while migration backfills.",
                "sql": "SELECT COUNT(*) AS violations FROM {schema}.TESTTABLE WHERE CC_OPEN_DATE_SK IS NULL;",
                "severity": "WARN",
            }
        ],
    },
    {
        "id": 2,
        "category": "Modelling",
        "name": "Dimension tables must have surrogate keys",
        "description": "All dimension tables must use surrogate keys (SK) as primary identifiers, not natural keys.",
        "pseudo_code": "All DIM_* tables must have a column ending in _SK as primary key",
        "assigned_to": "Analytics Engineering",
        "date_decided": date(2025, 10, 1),
        "date_enforced": date(2025, 10, 15),
        "test_sql": None,  # No automated test yet
        "status": "Active",
        "severity": "WARN",
        "exceptions": [],
    },
    {
        "id": 3,
        "category": "Info_Mart",
        "name": "Metrics must have date grain specified",
        "description": "All metrics in the info mart layer must clearly specify their date grain (daily, weekly, monthly).",
        "pseudo_code": "DAILY_METRICS must only have one row per METRIC_DATE + METRIC_NAME + REGION",
        "assigned_to": "BI Team",
        "date_decided": date(2025, 12, 1),
        "date_enforced": date(2026, 1, 15),
        "test_sql": """SELECT METRIC_DATE, METRIC_NAME, REGION, COUNT(*) as cnt 
FROM {schema}.DAILY_METRICS 
GROUP BY METRIC_DATE, METRIC_NAME, REGION 
HAVING COUNT(*) > 1;""",
        "status": "Active",
        "severity": "ERROR",
        "exceptions": [
            {
                "id": 1,
                "name": "QA duplicate tolerance",
                "description": "Allow up to five duplicates per metric while QA team finishes validation.",
                "sql": """SELECT METRIC_DATE, METRIC_NAME, REGION, COUNT(*) AS violations
FROM {schema}.DAILY_METRICS
GROUP BY METRIC_DATE, METRIC_NAME, REGION
HAVING COUNT(*) BETWEEN 1 AND 5;""",
                "severity": "INFO",
            }
        ],
    },
    {
        "id": 4,
        "category": "Governance",
        "name": "PII columns must be tagged",
        "description": "All columns containing PII must have appropriate tags applied for compliance tracking.",
        "pseudo_code": "Query information_schema for PII-named columns without tags → Should return 0",
        "assigned_to": "Data Governance",
        "date_decided": date(2026, 1, 10),
        "date_enforced": date(2026, 2, 1),
        "test_sql": None,  # Future enforcement
        "status": "Pending",
        "severity": "ERROR",
        "exceptions": [],
    },
]


def ensure_decision_defaults(decision: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure a decision dict has severity and exception defaults."""
    decision.setdefault("severity", DEFAULT_DECISION_ALERT_LEVEL)
    exceptions = decision.setdefault("exceptions", [])
    for exception in exceptions:
        exception.setdefault("severity", DEFAULT_EXCEPTION_ALERT_LEVEL)
    return decision


def get_decisions() -> List[Dict[str, Any]]:
    """
    Get all decisions from session state or initialize with samples.
    
    In a production app, this would query from a database table.
    """
    if "decisions" not in st.session_state:
        st.session_state.decisions = [ensure_decision_defaults(copy.deepcopy(d)) for d in SAMPLE_DECISIONS]
    else:
        for decision in st.session_state.decisions:
            ensure_decision_defaults(decision)
    return st.session_state.decisions


def add_decision(decision: Dict[str, Any]) -> None:
    """Add a new decision to session state."""
    decisions = get_decisions()
    decision["id"] = max([d["id"] for d in decisions], default=0) + 1
    decisions.append(ensure_decision_defaults(decision))
    st.session_state.decisions = decisions


def add_decision_exception(decision_id: int, exception: Dict[str, Any]) -> None:
    """Attach an exception SQL definition to a decision."""
    decisions = get_decisions()
    for decision in decisions:
        if decision["id"] == decision_id:
            exceptions = decision.setdefault("exceptions", [])
            next_id = max([exc.get("id", 0) for exc in exceptions], default=0) + 1
            exception["id"] = next_id
            exceptions.append(exception)
            break
    st.session_state.decisions = decisions


def run_decision_test(
    conn: Any,
    runtime_mode: str,
    test_sql: str,
    schema_prefix: str = "",
) -> tuple[bool, int, Optional[str]]:
    """
    Run a decision validation test.
    
    Args:
        conn: Database connection
        runtime_mode: Current runtime mode
        test_sql: SQL query to run (should return 0 rows/count for pass)
        schema_prefix: Schema prefix for {schema} placeholder
    
    Returns:
        Tuple of (passed: bool, violation_count: int, error_message: Optional[str])
    """
    try:
        # Replace {schema} placeholder
        formatted_sql = test_sql.format(schema=schema_prefix) if schema_prefix else test_sql.replace("{schema}.", "")
        
        df = run_query(conn, formatted_sql, runtime_mode)
        
        # Check results - test passes if 0 rows or count is 0
        if df.empty:
            return True, 0, None
        
        # If query returns a count column, check if it's 0
        if len(df.columns) == 1 and df.iloc[0, 0] == 0:
            return True, 0, None
        
        # Otherwise, count of rows is the violation count
        violation_count = len(df) if "violations" not in df.columns.str.lower() else int(df.iloc[0, 0])
        return violation_count == 0, violation_count, None
        
    except Exception as e:
        return False, -1, str(e)


def show_alert_message(level: Optional[str], message: str) -> None:
    """Display a message using the Streamlit style that matches the severity."""
    severity = (level or DEFAULT_DECISION_ALERT_LEVEL).upper()
    if severity == "INFO":
        st.info(message)
    elif severity == "WARN":
        st.warning(message)
    else:
        st.error(message)


def render_exception_section(decision: Dict[str, Any], conn: Any, runtime_mode: str, schema_prefix: str) -> None:
    """Render existing exceptions and a form to add new ones."""
    st.markdown("**Exceptions & Managed Allowances**")
    exceptions = decision.get("exceptions") or []

    if exceptions:
        for exception in exceptions:
            label = exception.get("name", "Exception")
            severity = exception.get("severity", DEFAULT_EXCEPTION_ALERT_LEVEL)
            with st.expander(f"⚖️ {label} ({severity})", expanded=False):
                description = exception.get("description")
                if description:
                    st.markdown(description)
                sql_text = exception.get("sql")
                if sql_text:
                    st.code(sql_text, language="sql")
                else:
                    st.info("No SQL defined for this exception yet.")

                if sql_text and st.button(
                    "▶️ Run Exception Check",
                    key=f"run_exception_{decision['id']}_{exception.get('id', label)}",
                ):
                    with st.spinner("Running exception SQL..."):
                        _passed, violations, error = run_decision_test(
                            conn,
                            runtime_mode,
                            sql_text,
                            schema_prefix,
                        )
                        if error:
                            st.error(f"❌ Exception execution error: {error}")
                        elif violations == 0:
                            st.success("✅ No exception rows returned.")
                        else:
                            show_alert_message(
                                severity,
                                f"⚠️ Exception returned {violations} row(s). Review before escalation.",
                            )
    else:
        st.info("No exceptions have been recorded for this decision.")

    with st.expander("➕ Add Exception", expanded=False):
        form_id = f"add_exception_form_{decision['id']}"
        with st.form(form_id):
            exc_name = st.text_input(
                "Exception Name *",
                key=f"exc_name_{decision['id']}",
                placeholder="e.g., Legacy contract gap",
            )
            exc_description = st.text_area(
                "Description",
                key=f"exc_description_{decision['id']}",
                placeholder="Explain why this exception is allowed and any owners.",
                height=80,
            )
            exc_sql = st.text_area(
                "Exception SQL *",
                key=f"exc_sql_{decision['id']}",
                placeholder="SQL that returns rows to track exception scope...",
                height=120,
            )
            exc_severity = st.selectbox(
                "Alert Level",
                ALERT_LEVELS,
                index=ALERT_LEVELS.index(DEFAULT_EXCEPTION_ALERT_LEVEL),
                key=f"exc_severity_{decision['id']}",
            )

            submitted = st.form_submit_button("Add Exception", type="secondary")

        if submitted:
            if not exc_name or not exc_sql.strip():
                st.error("Exception name and SQL are required.")
            else:
                add_decision_exception(
                    decision_id=decision["id"],
                    exception={
                        "name": exc_name,
                        "description": exc_description or None,
                        "sql": exc_sql.strip(),
                        "severity": exc_severity,
                    },
                )
                st.success(f"Exception '{exc_name}' added.")


def render_decision_card(decision: Dict[str, Any], conn: Any, runtime_mode: str, schema_prefix: str) -> None:
    """Render a single decision card with test execution capability."""
    
    with st.expander(f"**{decision['name']}** ({decision['category']})", expanded=False):
        # Status badge
        status = decision.get("status", "Active")
        alert_level = decision.get("severity", DEFAULT_DECISION_ALERT_LEVEL)
        if status == "Active":
            st.success(f"Status: {status}")
        elif status == "Pending":
            st.warning(f"Status: {status}")
        else:
            st.info(f"Status: {status}")
        severity_icon = {"INFO": "ℹ️", "WARN": "⚠️", "ERROR": "🚨"}.get(alert_level.upper(), "⚠️")
        st.caption(f"{severity_icon} Alert Level: {alert_level.upper()}")
        
        # Decision details
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Assigned to:** {decision.get('assigned_to', 'Unassigned')}")
            st.markdown(f"**Date Decided:** {decision.get('date_decided', 'N/A')}")
        
        with col2:
            st.markdown(f"**Category:** {decision['category']}")
            enforced = decision.get('date_enforced')
            if enforced:
                if enforced <= date.today():
                    st.markdown(f"**Enforced Since:** {enforced} ✅")
                else:
                    st.markdown(f"**Enforcement Date:** {enforced} ⏳")
            else:
                st.markdown("**Enforcement Date:** Not set")
        
        st.markdown("---")
        
        st.markdown("**Description:**")
        st.markdown(decision.get('description', 'No description provided.'))
        
        st.markdown("**Pseudo Code / Logic:**")
        st.code(decision.get('pseudo_code', 'No pseudo code provided.'), language="text")
        
        # Test SQL section
        test_sql = decision.get('test_sql')
        if test_sql:
            st.markdown("**Validation Test SQL:**")
            st.code(test_sql, language="sql")
            
            # Run test button
            if st.button(f"▶️ Run Test", key=f"run_test_{decision['id']}"):
                with st.spinner("Running validation test..."):
                    passed, violations, error = run_decision_test(conn, runtime_mode, test_sql, schema_prefix)
                    
                    if error:
                        st.error(f"❌ Test Error: {error}")
                    elif passed:
                        st.success(f"✅ Test PASSED - 0 violations found")
                    else:
                        show_alert_message(
                            alert_level,
                            f"❌ Test flagged - {violations} violation(s) found",
                        )
        else:
            st.info("📝 No automated test configured for this decision.")

        render_exception_section(decision, conn, runtime_mode, schema_prefix)


def render_add_decision_form() -> Optional[Dict[str, Any]]:
    """Render the form for adding a new decision."""
    
    with st.form("add_decision_form"):
        st.subheader("Add New Decision")
        
        col1, col2 = st.columns(2)
        
        with col1:
            name = st.text_input("Decision Name *", placeholder="e.g., No duplicate records in fact tables")
            category = st.selectbox("Category *", DECISION_CATEGORIES)
            assigned_to = st.text_input("Assigned To", placeholder="e.g., Data Engineering Team")
        
        with col2:
            date_decided = st.date_input("Date Decided", value=date.today())
            date_enforced = st.date_input("Enforcement Date", value=date.today())
            status = st.selectbox("Status", ["Active", "Pending", "Draft", "Deprecated"])
            alert_level = st.selectbox(
                "Alert Level",
                ALERT_LEVELS,
                index=ALERT_LEVELS.index(DEFAULT_DECISION_ALERT_LEVEL),
            )
        
        description = st.text_area(
            "Description *",
            placeholder="Describe the decision and its rationale...",
            height=100,
        )
        
        pseudo_code = st.text_area(
            "Pseudo Code / Logic",
            placeholder="Describe the validation logic in plain language...",
            height=80,
        )
        
        test_sql = st.text_area(
            "Validation Test SQL",
            placeholder="""Enter SQL that should return 0 rows when the decision is being followed.
Use {schema} as a placeholder for the schema prefix.

Example:
SELECT * FROM {schema}.MY_TABLE WHERE some_column IS NULL;""",
            height=120,
        )
        
        submitted = st.form_submit_button("➕ Add Decision", type="primary")
        
        if submitted:
            if not name or not description:
                st.error("Please fill in required fields (Name and Description)")
                return None
            
            return {
                "category": category,
                "name": name,
                "description": description,
                "pseudo_code": pseudo_code or None,
                "assigned_to": assigned_to or "Unassigned",
                "date_decided": date_decided,
                "date_enforced": date_enforced,
                "test_sql": test_sql if test_sql.strip() else None,
                "status": status,
                "severity": alert_level,
                "exceptions": [],
            }
    
    return None


def main():
    """Main function for Decisions page."""
    
    # Initialize page
    conn = init_page()
    runtime_mode = get_runtime_mode()
    
    # Get schema prefix for Snowflake
    if runtime_mode in ("snowflake_local", "snowflake_deployed"):
        schema_prefix = get_snowflake_table_prefix()
    else:
        schema_prefix = ""
    
    # Page header
    st.title("📋 Decisions")
    st.markdown("Log and validate architectural decisions across the data platform.")
    
    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["📚 All Decisions", "➕ Add Decision", "🧪 Run All Tests"])
    
    with tab1:
        # Filter controls
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            filter_category = st.selectbox(
                "Filter by Category",
                ["All"] + DECISION_CATEGORIES,
                key="filter_cat",
            )
        
        with col2:
            filter_status = st.selectbox(
                "Filter by Status",
                ["All", "Active", "Pending", "Draft", "Deprecated"],
                key="filter_status",
            )
        
        with col3:
            show_only_testable = st.checkbox("Has Test", value=False)
        
        st.markdown("---")
        
        # Get and filter decisions
        decisions = get_decisions()
        
        filtered = decisions
        if filter_category != "All":
            filtered = [d for d in filtered if d["category"] == filter_category]
        if filter_status != "All":
            filtered = [d for d in filtered if d.get("status") == filter_status]
        if show_only_testable:
            filtered = [d for d in filtered if d.get("test_sql")]
        
        # Summary metrics
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Total Decisions", len(decisions))
        with m2:
            st.metric("Active", len([d for d in decisions if d.get("status") == "Active"]))
        with m3:
            st.metric("With Tests", len([d for d in decisions if d.get("test_sql")]))
        with m4:
            pending_enforcement = len([
                d for d in decisions 
                if d.get("date_enforced") and d.get("date_enforced") > date.today()
            ])
            st.metric("Pending Enforcement", pending_enforcement)
        
        st.markdown("---")
        
        # Render decisions by category
        if not filtered:
            st.info("No decisions match the current filters.")
        else:
            # Group by category
            categories_present = sorted(set(d["category"] for d in filtered))
            
            for category in categories_present:
                st.subheader(f"📁 {category}")
                category_decisions = [d for d in filtered if d["category"] == category]
                
                for decision in category_decisions:
                    render_decision_card(decision, conn, runtime_mode, schema_prefix)
                
                st.markdown("")  # Spacing
    
    with tab2:
        new_decision = render_add_decision_form()
        
        if new_decision:
            add_decision(new_decision)
            st.success(f"✅ Decision '{new_decision['name']}' added successfully!")
            st.balloons()
    
    with tab3:
        st.subheader("🧪 Run All Validation Tests")
        st.markdown("Execute all configured validation tests and see results.")
        
        decisions = get_decisions()
        testable = [d for d in decisions if d.get("test_sql")]
        
        if not testable:
            st.info("No decisions have validation tests configured.")
        else:
            st.markdown(f"**{len(testable)} decisions** have validation tests configured.")
            
            if st.button("▶️ Run All Tests", type="primary"):
                st.markdown("---")
                
                results = []
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i, decision in enumerate(testable):
                    status_text.text(f"Testing: {decision['name']}...")
                    
                    passed, violations, error = run_decision_test(
                        conn, runtime_mode, decision["test_sql"], schema_prefix
                    )
                    severity = decision.get("severity", DEFAULT_DECISION_ALERT_LEVEL)
                    status_label = "✅ PASS" if passed and not error else f"{severity.upper()} ⚠️"
                    if error:
                        status_label = "❌ ERROR"
                    
                    results.append({
                        "Decision": decision["name"],
                        "Category": decision["category"],
                        "Alert Level": severity,
                        "Status": status_label,
                        "Violations": violations if not error else "Error",
                        "Error": error or "",
                        "Passed": passed and not error,
                    })
                    
                    progress_bar.progress((i + 1) / len(testable))
                
                status_text.text("All tests complete!")
                
                # Show results
                st.markdown("### Test Results")
                
                passed_count = len([r for r in results if r["Passed"]])
                failed_count = len(results) - passed_count
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Passed", passed_count, delta=None)
                with col2:
                    st.metric("Failed", failed_count, delta=None, delta_color="inverse")
                
                # Results table
                import pandas as pd
                results_df = pd.DataFrame(results)
                if "Passed" in results_df.columns:
                    results_df = results_df.drop(columns=["Passed"])  # Display friendly columns only
                
                # Color the status column
                st.dataframe(
                    results_df,
                    width="stretch",
                    hide_index=True,
                )
                
                # Show failures detail
                failures = [r for r in results if not r["Passed"]]
                if failures:
                    st.markdown("### ❌ Failed Tests Detail")
                    for failure in failures:
                        with st.expander(f"{failure['Decision']} ({failure['Category']})"):
                            if failure["Error"]:
                                st.error(f"Error: {failure['Error']}")
                            else:
                                show_alert_message(
                                    failure.get("Alert Level", DEFAULT_DECISION_ALERT_LEVEL),
                                    f"Violations found: {failure['Violations']}",
                                )


# Run the page
main()
