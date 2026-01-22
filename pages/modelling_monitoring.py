"""
Modelling Monitoring page

Monitor data transformation and modelling pipelines.

Future features:
- dbt model status
- Transformation lineage
- Model run history
- Data quality metrics
"""

import streamlit as st
from common.layout import init_page, get_runtime_mode


def main():
    """Main function for Modelling Monitoring page."""
    
    # Initialize page
    conn = init_page()
    runtime_mode = get_runtime_mode()
    
    # Page header
    st.title("🔄 Modelling Monitoring")
    st.markdown("Monitor data transformation and modelling pipelines.")
    
    # Placeholder content
    st.info("🚧 **Coming Soon** - This page is under development.")
    
    # Feature preview
    st.markdown("### Planned Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Model Status**
        - dbt model runs
        - Transformation health
        - Dependency tracking
        - Build times
        """)
        
        st.markdown("""
        **Data Lineage**
        - Source to target mapping
        - Column-level lineage
        - Impact analysis
        - Dependency graphs
        """)
    
    with col2:
        st.markdown("""
        **Run History**
        - Historical runs
        - Success/failure trends
        - Performance over time
        - Resource usage
        """)
        
        st.markdown("""
        **Quality Metrics**
        - Test results
        - Data freshness
        - Schema validation
        - Anomaly detection
        """)
    
    st.markdown("---")
    
    # Demo placeholder metrics
    st.subheader("📊 Sample Metrics (Demo)")
    
    m1, m2, m3, m4 = st.columns(4)
    
    with m1:
        st.metric("Active Models", "156", delta="8")
    with m2:
        st.metric("Runs Today", "2,341", delta="456")
    with m3:
        st.metric("Test Pass Rate", "97.8%", delta="0.5%")
    with m4:
        st.metric("Avg Build Time", "4.2m", delta="-0.8m")


# Run the page
main()
