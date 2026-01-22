"""
Alerting page

Configure and manage alerts for data pipeline issues.

Future features:
- Alert configuration
- Notification channels
- Alert history
- Escalation policies
"""

import streamlit as st
from common.layout import init_page, get_runtime_mode


def main():
    """Main function for Alerting page."""
    
    # Initialize page
    conn = init_page()
    runtime_mode = get_runtime_mode()
    
    # Page header
    st.title("🔔 Alerting")
    st.markdown("Configure and manage alerts for data pipeline issues.")
    
    # Placeholder content
    st.info("🚧 **Coming Soon** - This page is under development.")
    
    # Feature preview
    st.markdown("### Planned Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Alert Configuration**
        - Threshold alerts
        - Anomaly detection
        - Custom conditions
        - Alert templates
        """)
        
        st.markdown("""
        **Notification Channels**
        - Email notifications
        - Slack integration
        - PagerDuty
        - Webhooks
        """)
    
    with col2:
        st.markdown("""
        **Alert History**
        - Recent alerts
        - Resolution tracking
        - Trend analysis
        - Root cause linking
        """)
        
        st.markdown("""
        **Escalation**
        - Escalation policies
        - On-call schedules
        - Acknowledgment tracking
        - SLA monitoring
        """)
    
    st.markdown("---")
    
    # Demo placeholder - Recent Alerts
    st.subheader("🚨 Recent Alerts (Demo)")
    
    # Sample alert data
    alerts = [
        {"severity": "🔴", "alert": "Load job failed: ORDERS_DAILY", "time": "5 min ago", "status": "Open"},
        {"severity": "🟡", "alert": "High latency: INVENTORY_SYNC", "time": "23 min ago", "status": "Investigating"},
        {"severity": "🟢", "alert": "Schema change detected: CUSTOMERS", "time": "1 hour ago", "status": "Resolved"},
        {"severity": "🟡", "alert": "Data quality warning: NULL values in PRODUCT_ID", "time": "2 hours ago", "status": "Open"},
    ]
    
    for alert in alerts:
        col1, col2, col3, col4 = st.columns([1, 6, 2, 2])
        with col1:
            st.write(alert["severity"])
        with col2:
            st.write(alert["alert"])
        with col3:
            st.caption(alert["time"])
        with col4:
            st.caption(alert["status"])


# Run the page
main()
