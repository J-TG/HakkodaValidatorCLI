"""Reusable SQL query definitions for the Migration Copilot app.

This module centralizes named SQL snippets so that application pages
can reference the same definition for execution (e.g., metrics) and
for display inside the "Query Settings" system section.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from common.db import RuntimeMode, get_ingestion_metadata_table


@dataclass(frozen=True)
class QueryDefinition:
    """Stores metadata and SQL template for a reusable query."""

    key: str
    name: str
    description: str
    sql_template: str
    category: str = "metrics"

    def render(self, mode: RuntimeMode) -> str:
        """Render the SQL template using the correct table reference."""
        table_ref = get_ingestion_metadata_table(mode)
        return self.sql_template.format(table_ref=table_ref)


def _build_query_catalog() -> Dict[str, QueryDefinition]:
    """Define all reusable queries in one place."""
    return {
        "active_pipelines": QueryDefinition(
            key="active_pipelines",
            name="Active Pipelines",
            description="Counts distinct pipelines where ENABLED is set to 1.",
            sql_template="""
SELECT COUNT(DISTINCT METADATA_CONFIG_KEY) AS pipeline_count
FROM {table_ref}
WHERE COALESCE(TRIM(ENABLED), '0') = '1';
""".strip(),
        ),
        "inactive_pipelines": QueryDefinition(
            key="inactive_pipelines",
            name="Inactive Pipelines",
            description="Counts distinct pipelines where ENABLED is set to 0.",
            sql_template="""
SELECT COUNT(DISTINCT METADATA_CONFIG_KEY) AS pipeline_count
FROM {table_ref}
WHERE COALESCE(TRIM(ENABLED), '0') = '0';
""".strip(),
        ),
    }


QUERY_DEFINITIONS: Dict[str, QueryDefinition] = _build_query_catalog()
METRIC_QUERY_KEYS = (
    "active_pipelines",
    "inactive_pipelines",
)


def get_query_definition(key: str) -> QueryDefinition:
    """Retrieve a query definition by key."""
    if key not in QUERY_DEFINITIONS:
        raise KeyError(f"Unknown query key: {key}")
    return QUERY_DEFINITIONS[key]


def list_queries(category: Optional[str] = None) -> List[QueryDefinition]:
    """List all queries, optionally filtered by category."""
    queries = QUERY_DEFINITIONS.values()
    if category:
        return [q for q in queries if q.category == category]
    return list(queries)
