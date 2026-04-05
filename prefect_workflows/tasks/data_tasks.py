"""
Data Tasks - Data Processing & Enrichment

Tasks for deduplication, enrichment, and data transformation.
"""

from datetime import timedelta
from difflib import SequenceMatcher
from typing import Optional
from urllib.parse import urlparse

from prefect import task, get_run_logger
from prefect.tasks import task_input_hash

from prefect_workflows.models import (
    RawCandidate,
    Candidate,
    ThirdPartySource,
    LeadDiscoverySite,
    Classification,
    CandidateCategory,
    SourceType,
)


# ============================================================================
# DEDUPLICATION TASKS
# ============================================================================

@task(
    name="deduplicate_candidates",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def deduplicate_candidates(
    raw_candidates: list[list[RawCandidate]],
    exclusion_list: list[str] = None
) -> list[Candidate]:
    """
    Deduplicate and merge raw candidates from multiple sources.
    
    Args:
        raw_candidates: Nested list from mapped task results
        exclusion_list: Names to exclude (e.g., seed products)
    """
    logger = get_run_logger()
    
    # Flatten the nested list
    all_raw = []
    for batch in raw_candidates:
        all_raw.extend(batch)
    
    logger.info(f"Deduplicating {len(all_raw)} raw candidates")
    
    exclusion_list = exclusion_list or []
    exclusion_lower = [e.lower().strip() for e in exclusion_list]
    
    # Group by normalized name
    name_groups: dict[str, list[RawCandidate]] = {}
    
    for raw in all_raw:
        # Normalize name
        normalized = _normalize_name(raw.name)
        
        # Skip if excluded
        if normalized.lower() in exclusion_lower:
            continue
        
        if normalized not in name_groups:
            name_groups[normalized] = []
        name_groups[normalized].append(raw)
    
    # Merge each group into a Candidate
    candidates = []
    for normalized_name, group in name_groups.items():
        # Use the most common variation as the display name
        display_name = _most_common([g.name for g in group])
        
        # Collect all discovery sources and evidence
        discovery_sources = list(set([g.discovery_source for g in group]))
        evidence_links = list(set([g.evidence_url for g in group]))
        
        # Use highest confidence
        max_confidence = max([g.confidence_score for g in group])
        
        candidate = Candidate(
            name=display_name,
            description="",  # Will be enriched later
            discovery_sources=discovery_sources,
            evidence_links=evidence_links,
        )
        
        candidates.append(candidate)
    
    logger.info(f"Deduplicated to {len(candidates)} unique candidates")
    return candidates


@task(
    name="enrich_candidates",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(days=1),
)
async def enrich_candidates(
    candidates: list[Candidate],
    max_candidates: int = 50
) -> list[Candidate]:
    """
    Enrich candidates with descriptions and website info.
    
    Limited to max_candidates to control scope.
    """
    logger = get_run_logger()
    
    # Sort by number of discovery sources (more sources = more likely to be important)
    sorted_candidates = sorted(
        candidates,
        key=lambda c: len(c.discovery_sources),
        reverse=True
    )
    
    # Limit
    to_enrich = sorted_candidates[:max_candidates]
    logger.info(f"Enriching top {len(to_enrich)} candidates")
    
    # Import here to avoid circular dependency
    from prefect_workflows.tasks.search_tasks import discover_product_website
    
    enriched = []
    for candidate in to_enrich:
        # Find website
        website = await discover_product_website(candidate.name)
        
        # Build description from evidence
        description = _build_description_from_evidence(candidate)
        
        # Extract features from description (simple heuristic)
        key_features = _extract_features_from_description(description)
        
        enriched_candidate = Candidate(
            id=candidate.id,
            name=candidate.name,
            website=website,
            description=description,
            discovery_sources=candidate.discovery_sources,
            evidence_links=candidate.evidence_links,
            first_discovered_at=candidate.first_discovered_at,
            key_features=key_features,
        )
        
        enriched.append(enriched_candidate)
    
    logger.info(f"Enriched {len(enriched)} candidates")
    return enriched


# ============================================================================
# SOURCE AGGREGATION TASKS
# ============================================================================

@task(
    name="rank_source_sites",
    cache_policy=task_input_hash,
)
def rank_source_sites(
    sources: list[list[ThirdPartySource]]
) -> list[LeadDiscoverySite]:
    """
    Aggregate and rank third-party sites by usefulness.
    
    Args:
        sources: Nested list from mapped task results
    """
    logger = get_run_logger()
    
    # Flatten
    all_sources = []
    for batch in sources:
        all_sources.extend(batch)
    
    logger.info(f"Ranking {len(all_sources)} sources")
    
    # Group by site name
    site_groups: dict[str, list[ThirdPartySource]] = {}
    for source in all_sources:
        if source.site_name not in site_groups:
            site_groups[source.site_name] = []
        site_groups[source.site_name].append(source)
    
    # Score each site
    ranked_sites = []
    for site_name, site_sources in site_groups.items():
        # Count unique candidates
        unique_candidates = list(set([s.candidate_name for s in site_sources]))
        
        # Calculate usefulness score
        # Based on: number of candidates, average relevance, site type
        avg_relevance = sum([s.relevance_score for s in site_sources]) / len(site_sources)
        candidate_count = len(unique_candidates)
        
        # Boost for discovery sites
        is_discovery = any([s.is_discovery_site for s in site_sources])
        discovery_boost = 0.3 if is_discovery else 0.0
        
        usefulness = (candidate_count * 0.1) + (avg_relevance * 0.5) + discovery_boost
        usefulness = min(1.0, usefulness)
        
        # Get URL pattern (use first source as example)
        url_pattern = site_sources[0].url if site_sources else ""
        
        # Extract site type
        site_type = site_sources[0].site_type if site_sources else SourceType.MEDIA
        
        ranked_sites.append(LeadDiscoverySite(
            site_name=site_name,
            url_pattern=url_pattern,
            site_type=site_type,
            candidates_found_count=candidate_count,
            unique_candidates=unique_candidates,
            usefulness_score=usefulness,
            notes=f"Found {candidate_count} candidates, avg relevance {avg_relevance:.2f}"
        ))
    
    # Sort by usefulness
    ranked_sites.sort(key=lambda s: s.usefulness_score, reverse=True)
    
    logger.info(f"Ranked {len(ranked_sites)} source sites")
    return ranked_sites


# ============================================================================
# CLASSIFICATION FILTERING
# ============================================================================

@task(
    name="filter_approved_candidates",
    cache_policy=task_input_hash,
)
def filter_approved_candidates(
    candidates: list[Candidate],
    classifications: list[Classification],
    user_review: Optional[dict] = None
) -> list[Candidate]:
    """
    Filter candidates based on classification and user review.
    
    Removes:
    - NOT_RELEVANT candidates
    - Candidates user marked as removed
    - User can reclassify
    """
    logger = get_run_logger()
    
    # Create lookup by candidate_id
    classification_map = {c.candidate_id: c for c in classifications}
    
    approved = []
    for candidate in candidates:
        classification = classification_map.get(candidate.id)
        
        if not classification:
            logger.warning(f"No classification for {candidate.name}, skipping")
            continue
        
        # Skip if user marked as removed
        if user_review and candidate.id in user_review.get("removed_ids", []):
            logger.info(f"User removed: {candidate.name}")
            continue
        
        # Check if user modified classification
        final_category = classification.category
        if user_review and "modified_classifications" in user_review:
            for mod in user_review["modified_classifications"]:
                if mod["candidate_id"] == candidate.id:
                    final_category = CandidateCategory(mod["new_category"])
                    logger.info(f"User reclassified {candidate.name} to {final_category.value}")
                    break
        
        # Skip NOT_RELEVANT (unless user overrode)
        if final_category == CandidateCategory.NOT_RELEVANT:
            logger.debug(f"Skipping not relevant: {candidate.name}")
            continue
        
        approved.append(candidate)
    
    logger.info(f"Approved {len(approved)} candidates after filtering")
    return approved


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _normalize_name(name: str) -> str:
    """Normalize a product name for deduplication."""
    # Lowercase
    normalized = name.lower().strip()
    
    # Remove common suffixes
    suffixes = [" software", " platform", " tool", " app", " service", " solution"]
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
    
    # Remove special chars (keep alphanumeric and spaces)
    normalized = "".join(c for c in normalized if c.isalnum() or c.isspace())
    
    return normalized.strip()


def _similarity(a: str, b: str) -> float:
    """Calculate string similarity."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _most_common(items: list[str]) -> str:
    """Return the most common item in a list."""
    from collections import Counter
    if not items:
        return ""
    counter = Counter(items)
    return counter.most_common(1)[0][0]


def _build_description_from_evidence(candidate: Candidate) -> str:
    """Build a description from evidence snippets."""
    # In a real implementation, this might use LLM to synthesize
    # For now, use the first evidence snippet
    if candidate.evidence_links:
        return f"Discovered from {len(candidate.discovery_sources)} sources including {candidate.evidence_links[0]}"
    return f"Discovered as potential competitor in {candidate.discovery_sources[0] if candidate.discovery_sources else 'search'}"


def _extract_features_from_description(description: str) -> list[str]:
    """Extract key features from a description."""
    # Simple keyword extraction
    feature_keywords = [
        "AI", "automation", "cloud", "on-premise", "API", "integration",
        "analytics", "dashboard", "collaboration", "workflow", "mobile",
        "web-based", "desktop", "open source", "SaaS", "enterprise",
        "SMB", "freemium", "real-time", "async"
    ]
    
    found = []
    desc_lower = description.lower()
    for keyword in feature_keywords:
        if keyword.lower() in desc_lower:
            found.append(keyword)
    
    return found[:5]  # Limit to top 5
