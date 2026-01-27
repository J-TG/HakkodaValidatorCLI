"""
Navigation configuration for the Migration Copilot app.

This module defines the page structure and navigation for the multipage app.
Using st.navigation (recommended method) for maximum flexibility.

Page Structure:
- Overview: Setup
- Ingestion: Ingestion Copilot, Validation Ingestion, Ingestion Monitoring
- Operations: Governance, Alerting, Decisions
- Compatibility: Compatibility Views
- Validation: Modelling, Model Monitoring
- System: Settings, Query Settings

To add a new page:
1. Create a new .py file in the pages/ directory
2. Add the page to the appropriate section in get_pages()
3. The page will automatically appear in the navigation
"""

import streamlit as st
from typing import Dict, List


def get_pages() -> Dict[str, List[st.Page]]:
    """
    Define all pages for the app, organized by section.
    
    Returns a dict where keys are section names and values are lists of st.Page objects.
    This structure enables grouped navigation in the sidebar.
    
    Note: Paths are relative to the entrypoint file (streamlit_app.py).
    """
    
    pages = {
        "Overview": [
            st.Page(
                "pages/home.py",
                title="Setup",
                icon="🏠",
                default=True,
            ),
        ],
        "Ingestion": [
            st.Page(
                "pages/admin_copilot.py",
                title="Ingestion Copilot",
                icon="🤖",
            ),
            st.Page(
                "pages/ingestion_monitoring.py",
                title="Ingestion Monitoring",
                icon="📥",
            ),
            st.Page(
                "pages/log_comparison.py",
                title="Log Comparison",
                icon="📊",
            ),
            st.Page(
                "pages/validation_ingestion.py",
                title="Ingestion Validation",
                icon="🧪",
            ),
        ],
        "Compatibility": [
            st.Page(
                "pages/compatibility_views.py",
                title="Projection Creation",
                icon="🧩",
            ),
            st.Page(
                "pages/reporting_views.py",
                title="Reporting View Creation",
                icon="🧾",
            ),
        ],
        "Validation": [
            st.Page(
                "pages/validation_modelling.py",
                title="Modelling",
                icon="🧠",
            ),
            st.Page(
                "pages/modelling_monitoring.py",
                title="Model Monitoring",
                icon="📊",
            ),
        ],
        "Operations": [
            st.Page(
                "pages/governance.py",
                title="Governance",
                icon="🛡️",
            ),
            st.Page(
                "pages/alerting.py",
                title="Alerting",
                icon="🔔",
            ),
            st.Page(
                "pages/decisions.py",
                title="Decisions",
                icon="📋",
            ),
        ],
        "System": [
            st.Page(
                "pages/settings.py",
                title="Settings",
                icon="⚙️",
            ),
            st.Page(
                "pages/query_settings.py",
                title="Query Settings",
                icon="🧰",
            ),
        ],
    }
    
    return pages


def setup_navigation() -> st.Page:
    """
    Set up the navigation and return the selected page.
    
    This should be called from the entrypoint file (streamlit_app.py).
    The returned page should then be run with page.run().
    
    Returns:
        The currently selected st.Page object
    """
    pages = get_pages()
    
    # st.navigation accepts a dict for grouped navigation
    # Keys become section headers in the sidebar
    nav = st.navigation(pages)
    
    return nav
