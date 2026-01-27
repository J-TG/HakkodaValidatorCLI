"""Query Settings page for viewing reusable SQL definitions."""

import streamlit as st

from common.layout import init_page, get_runtime_mode
from common.query_library import list_queries
from common.db import get_ingestion_metadata_table_path, get_ingestion_metadata_env


def main() -> None:
    """Render the Query Settings system page."""
    init_page(require_connection=False)
    runtime_mode = get_runtime_mode()
    st.title("🛠️ Query Settings")
    st.caption(
        "Central catalog of saved SQL statements used by Copilot features."
    )
    active_env = get_ingestion_metadata_env()
    active_table = get_ingestion_metadata_table_path()
    st.info(
        f"Active ingestion environment: `{active_env}` · Table: `{active_table}`"
    )
    for query in list_queries():
        st.subheader(query.name)
        st.write(query.description)
        sql_preview = query.render(runtime_mode)
        st.text_area(
            label=f"SQL ({query.key})",
            value=sql_preview,
            height=140,
            key=f"query-settings-{query.key}",
            disabled=True,
        )
        st.markdown("---")


main()
