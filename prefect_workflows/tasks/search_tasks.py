"""
Search Tasks - Brave Search Integration

Prefect tasks for web search with caching and retry policies.
"""

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Optional

from prefect import task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.concurrency.asyncio import concurrency

# Import existing agents
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from agents.brave_search import brave_search as _brave_search_impl
from agents.utils import classify_url

from prefect_workflows.models import (
    SearchDirection,
    RawCandidate,
    ThirdPartySource,
    SourceType,
)


# ============================================================================
# CACHED SEARCH TASKS
# ============================================================================

@task(
    name="execute_discovery_search",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(hours=24),
    retries=3,
    retry_delay_seconds=[5, 15, 45],  # Exponential backoff
)
async def execute_discovery_search(direction: SearchDirection) -> list[RawCandidate]:
    """
    Execute a search direction and extract candidates.
    
    Cached for 24 hours - same query won't re-run.
    Retries on failure with exponential backoff.
    """
    logger = get_run_logger()
    logger.info(f"Searching: {direction.query}")
    
    # Limit concurrency to avoid rate limits
    async with concurrency("brave_api", occupy=1):
        results = await _brave_search_impl(direction.query, extra_snippets=False)
    
    if not results:
        logger.warning(f"No results for: {direction.query}")
        return []
    
    logger.info(f"Found {len(results)} results for: {direction.query}")
    
    # Extract candidates from search results
    candidates = []
    for result in results[:10]:  # Top 10 results
        # Extract product name from title/snippet
        title = result.get("title", "")
        snippet = result.get("description", "")
        url = result.get("url", "")
        
        # Simple extraction - in production, use LLM to extract company names
        candidate_name = _extract_product_name(title, snippet)
        
        if candidate_name and len(candidate_name) > 2:
            candidate = RawCandidate(
                name=candidate_name,
                discovery_source=direction.query,
                evidence_url=url,
                evidence_snippet=snippet[:300],
                confidence_score=_calculate_confidence(result, direction.source_type),
                search_direction_id=direction.id
            )
            candidates.append(candidate)
    
    logger.info(f"Extracted {len(candidates)} candidates from search")
    return candidates


@task(
    name="execute_source_search",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(hours=24),
    retries=3,
    retry_delay_seconds=[5, 15, 45],
)
async def execute_source_search(
    candidate_name: str,
    site_query: str
) -> list[ThirdPartySource]:
    """
    Search for a candidate on a specific site (G2, Capterra, etc.).
    """
    logger = get_run_logger()
    logger.info(f"Searching for '{candidate_name}' on: {site_query}")
    
    async with concurrency("brave_api", occupy=1):
        results = await _brave_search_impl(site_query, extra_snippets=False)
    
    if not results:
        return []
    
    sources = []
    for result in results[:5]:
        url = result.get("url", "")
        snippet = result.get("description", "")
        title = result.get("title", "")
        
        # Classify the URL
        source_type = classify_url(url, snippet)
        
        source = ThirdPartySource(
            candidate_id="",  # Set by caller
            candidate_name=candidate_name,
            url=url,
            site_name=_extract_site_name(url),
            site_type=SourceType(source_type),
            title=title[:200],
            snippet=snippet[:300],
            relevance_score=_calculate_source_relevance(result, candidate_name),
            is_discovery_site=_is_discovery_site(url)
        )
        sources.append(source)
    
    logger.info(f"Found {len(sources)} sources for {candidate_name}")
    return sources


@task(
    name="discover_product_website",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(days=7),
)
async def discover_product_website(product_name: str) -> Optional[str]:
    """
    Find the official website for a product.
    Cached for 7 days.
    """
    logger = get_run_logger()
    logger.info(f"Finding website for: {product_name}")
    
    query = f'"{product_name}" official website'
    
    async with concurrency("brave_api", occupy=1):
        results = await _brave_search_impl(query, count=5)
    
    if not results:
        return None
    
    # Return the first result that's likely the official site
    for result in results:
        url = result.get("url", "")
        title = result.get("title", "")
        
        # Heuristic: official sites often have product name in title
        if product_name.lower() in title.lower():
            return url
    
    # Fallback to first result
    return results[0].get("url") if results else None


# ============================================================================
# HELPERS
# ============================================================================

def _extract_product_name(title: str, snippet: str) -> str:
    """Extract product/company name from search result."""
    # Simple heuristic: take first 2-3 words of title before common separators
    separators = [" - ", " | ", " — ", ": ", " vs ", " vs. ", " alternative", " review"]
    
    name = title
    for sep in separators:
        if sep in name:
            name = name.split(sep)[0]
    
    # Clean up
    name = name.strip()
    
    # Remove common suffixes
    suffixes = [" software", " platform", " tool", " app", " service"]
    for suffix in suffixes:
        if name.lower().endswith(suffix):
            name = name[:-len(suffix)]
    
    return name.strip()[:100]


def _extract_site_name(url: str) -> str:
    """Extract site name from URL."""
    from urllib.parse import urlparse
    
    domain = urlparse(url).netloc.lower()
    
    # Remove www. and extract main domain
    if domain.startswith("www."):
        domain = domain[4:]
    
    # Map known domains to friendly names
    site_names = {
        "g2.com": "G2",
        "capterra.com": "Capterra",
        "trustpilot.com": "Trustpilot",
        "alternativeto.net": "AlternativeTo",
        "reddit.com": "Reddit",
        "producthunt.com": "Product Hunt",
        "getapp.com": "GetApp",
        "softwareadvice.com": "Software Advice",
        "slashdot.org": "Slashdot",
        "news.ycombinator.com": "Hacker News",
    }
    
    for domain_pattern, name in site_names.items():
        if domain_pattern in domain:
            return name
    
    # Fallback: capitalize domain
    return domain.split(".")[0].capitalize()


def _calculate_confidence(result: dict, source_type: SourceType) -> float:
    """Calculate confidence score for a raw candidate."""
    score = 0.5
    
    # Boost based on source type
    type_boosts = {
        SourceType.DIRECTORY: 0.2,
        SourceType.COMPARISON: 0.15,
        SourceType.ALTERNATIVE: 0.1,
        SourceType.REVIEW: 0.05,
        SourceType.MEDIA: 0.0,
    }
    score += type_boosts.get(source_type, 0)
    
    # Boost for clear product mentions in snippet
    snippet = result.get("description", "").lower()
    positive_indicators = ["software", "platform", "tool", "app", "product", "service"]
    for indicator in positive_indicators:
        if indicator in snippet:
            score += 0.02
    
    return min(0.95, score)


def _calculate_source_relevance(result: dict, candidate_name: str) -> float:
    """Calculate relevance of a third-party source."""
    score = 0.5
    
    title = result.get("title", "").lower()
    snippet = result.get("description", "").lower()
    candidate_lower = candidate_name.lower()
    
    # Boost if candidate name appears in title
    if candidate_lower in title:
        score += 0.3
    
    # Boost if in snippet
    if candidate_lower in snippet:
        score += 0.1
    
    # Boost for review/comparison keywords
    review_keywords = ["review", "rating", "stars", "compare", "vs", "alternative"]
    for kw in review_keywords:
        if kw in title or kw in snippet:
            score += 0.05
    
    return min(0.95, score)


def _is_discovery_site(url: str) -> bool:
    """Determine if this site can be used to discover MORE candidates."""
    discovery_domains = [
        "g2.com",
        "capterra.com",
        "alternativeto.net",
        "getapp.com",
        "softwareadvice.com",
        "producthunt.com",
        "stackshare.io",
    ]
    
    url_lower = url.lower()
    return any(domain in url_lower for domain in discovery_domains)
