"""
Prefect Workflows for Multi-Seed Market Discovery

A Prefect-based workflow system for discovering, classifying, and mapping
competitors starting from multiple seed products.

Key Features:
- Multi-seed market discovery
- 5 human-in-the-loop checkpoints
- Parallel candidate analysis
- Third-party source discovery
- Resume/pause capability
"""

from prefect_workflows.flows.market_discovery import market_discovery_flow
from prefect_workflows.models import (
    DiscoveryRequest,
    DiscoveryResult,
    SeedProduct,
    Candidate,
    Classification,
    SearchBreadth,
)

__all__ = [
    "market_discovery_flow",
    "DiscoveryRequest",
    "DiscoveryResult",
    "SeedProduct",
    "Candidate",
    "Classification",
    "SearchBreadth",
]
