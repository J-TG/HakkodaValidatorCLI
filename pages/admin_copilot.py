"""
Ingestion Copilot page.

AI-powered assistant for ingestion-focused monitoring and operations.
"""

import streamlit as st
from common.layout import init_page, get_runtime_mode, get_db_connection
from common.db import run_query, get_ingestion_metadata_table, get_ingestion_metadata_env
from common.query_library import get_query_definition, METRIC_QUERY_KEYS


def _fetch_metric_value(conn, runtime_mode, query_key):
    """Execute a metric query and return the numeric result."""
    query_def = get_query_definition(query_key)
    sql = query_def.render(runtime_mode)
    df = run_query(conn, sql, runtime_mode)
    if df.empty:
        return 0, query_def
    value = df.iloc[0, 0]
    if value is None:
        return 0, query_def
    try:
        return int(value), query_def
    except (TypeError, ValueError):
        return 0, query_def


def render_pipeline_metrics(conn, runtime_mode):
    """Show key ingestion metrics derived from metadata config."""
    st.markdown("### 📊 Pipeline Metrics")
    env = get_ingestion_metadata_env()
    table_ref = get_ingestion_metadata_table(runtime_mode)
    st.caption(f"Environment: `{env}` · Source table: `{table_ref}`")
    cols = st.columns(len(METRIC_QUERY_KEYS))
    for col, query_key in zip(cols, METRIC_QUERY_KEYS):
        try:
            value, query_def = _fetch_metric_value(conn, runtime_mode, query_key)
            with col:
                st.metric(label=query_def.name, value=f"{value:,}")
                st.caption(query_def.description)
        except Exception as exc:
            with col:
                st.error(f"{query_key} metric unavailable: {exc}")


def main():
    """Main function for Ingestion Copilot page."""
    
    # Initialize page
    conn = init_page()
    runtime_mode = get_runtime_mode()
    
    # Page header
    st.title("🤖 Ingestion Copilot")
    st.markdown("AI-powered assistant for ingestion pipeline tasks.")
    
    render_pipeline_metrics(conn, runtime_mode)
    
    st.divider()
    
    # Placeholder content
    st.info("🚧 **Coming Soon** - This page is under development.")
    
    # Feature preview
    st.markdown("### Planned Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Natural Language Interface**
        - Ask questions in plain English
        - Get SQL suggestions
        - Explain query results
        - Troubleshoot errors
        """)
        
        st.markdown("""
        **SQL Generation**
        - Generate queries from descriptions
        - Optimize existing queries
        - Explain query plans
        - Suggest indexes
        """)
    
    with col2:
        st.markdown("""
        **Troubleshooting**
        - Error diagnosis
        - Performance analysis
        - Root cause suggestions
        - Fix recommendations
        """)
        
        st.markdown("""
        **Documentation**
        - Schema documentation
        - Best practices
        - How-to guides
        - Context-aware help
        """)
    
    st.markdown("---")
    
    # Demo chat interface
    st.subheader("💬 Ask the Copilot (Demo)")
    
    # Initialize chat history
    if "copilot_messages" not in st.session_state:
        st.session_state.copilot_messages = [
            {"role": "assistant", "content": "Hello! I'm the Admin Copilot. How can I help you today? (Note: This is a demo - full AI integration coming soon!)"}
        ]
    
    # Display chat messages
    for message in st.session_state.copilot_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask me anything about your data..."):
        # Add user message
        st.session_state.copilot_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Demo response
        demo_response = f"""Thanks for your question about: *"{prompt}"*

This is a demo response. In the full version, I'll be able to:
- Generate SQL queries based on your request
- Explain data in your tables
- Help troubleshoot issues
- Provide documentation and best practices

**Coming soon!** 🚀"""
        
        st.session_state.copilot_messages.append({"role": "assistant", "content": demo_response})
        with st.chat_message("assistant"):
            st.markdown(demo_response)


# Run the page
main()
