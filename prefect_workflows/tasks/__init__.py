"""
Prefect Tasks

Individual tasks for search, LLM calls, and data processing.
"""

from prefect_workflows.tasks.search_tasks import (
    execute_discovery_search,
    execute_source_search,
    discover_product_website,
)
from prefect_workflows.tasks.llm_tasks import (
    analyze_seed_product,
    synthesize_market_understanding,
    generate_search_directions,
    classify_candidate,
)
from prefect_workflows.tasks.data_tasks import (
    deduplicate_candidates,
    enrich_candidates,
    rank_source_sites,
    filter_approved_candidates,
)

__all__ = [
    "execute_discovery_search",
    "execute_source_search",
    "discover_product_website",
    "analyze_seed_product",
    "synthesize_market_understanding",
    "generate_search_directions",
    "classify_candidate",
    "deduplicate_candidates",
    "enrich_candidates",
    "rank_source_sites",
    "filter_approved_candidates",
]
