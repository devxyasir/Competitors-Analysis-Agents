# Prefect-based Market Discovery Workflows

This directory contains Prefect flows and tasks for the multi-seed market discovery system.

## Structure

```
prefect_workflows/
├── flows/
│   └── market_discovery.py      # Main discovery flow with checkpoints
├── tasks/
│   ├── search_tasks.py          # Brave search, source discovery
│   ├── llm_tasks.py             # LLM extraction, classification
│   ├── crawl_tasks.py           # Web crawling
│   └── data_tasks.py            # Deduplication, enrichment
├── infrastructure/
│   ├── checkpoint_manager.py    # Pause/resume state management
│   ├── event_emitter.py         # Frontend notifications
│   └── cache_config.py          # Task caching policies
└── models.py                    # Prefect-specific data models
```

## Usage

```python
from prefect_workflows.flows.market_discovery import market_discovery_flow

# Start a new discovery
result = market_discovery_flow(
    seed_products=[{"name": "Perplexity Computer"}, {"name": "Manus"}],
    scope_config={"target_market": "AI assistant agents"},
    breadth="balanced",
    limits={"max_candidates": 50}
)
```

## Checkpoints

The flow has 5 checkpoints where it pauses for human review:

1. **Seed Analysis** - Review extracted seed profiles
2. **Search Directions** - Review generated search queries  
3. **Discovered Candidates** - Review raw candidates
4. **Classification** - CRITICAL: Review and correct classifications
5. **Sources** - Review discovered third-party sites

## Running Prefect Server

```bash
# Install Prefect
pip install prefect

# Start Prefect server
prefect server start

# Configure (optional)
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```
