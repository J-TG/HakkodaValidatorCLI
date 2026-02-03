"""
Migration Copilot - Entrypoint File

This is the main entrypoint for the Streamlit app. It acts as a router
using st.navigation (recommended method for multipage apps).

The entrypoint file:
1. Sets page configuration (must be first Streamlit command)
2. Initializes environment and session state
3. Sets up navigation with st.navigation
4. Runs the selected page

All page content is defined in the pages/ directory.
Common layout/frame elements are in common/layout.py.
Navigation configuration is in common/navigation.py.

For more info on multipage apps:
https://docs.snowflake.com/en/developer-guide/streamlit/app-development/file-organization
https://docs.streamlit.io/develop/concepts/multipage-apps/overview
"""

import streamlit as st
import traceback

# =============================================================================
# Page configuration (MUST be first Streamlit command)
# =============================================================================
st.set_page_config(
    page_title="Migration Copilot",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# Import after page config
# =============================================================================
from common.layout import init_environment, init_session_state
from common.navigation import setup_navigation

# =============================================================================
# Initialize environment and session state
# =============================================================================
init_environment()
init_session_state()

# =============================================================================
# Set up navigation and run selected page
# =============================================================================
# st.navigation returns the selected page
# The dict structure creates grouped navigation in the sidebar
try:
    page = setup_navigation()
    # Run the selected page
    page.run()
except Exception as exc:
    # Surface unexpected errors so the browser doesn't just disconnect
    traceback.print_exc()
    st.exception(exc)
    st.stop()
