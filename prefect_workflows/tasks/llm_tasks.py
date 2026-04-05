"""
LLM Tasks - AI Agent Integration

Prefect tasks for LLM calls with caching and retry policies.
"""

from datetime import timedelta
from typing import List, Optional

from prefect import task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.concurrency.asyncio import concurrency

# Import existing agents
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from agents.ai_agent import deep_crawl, LONGCAT_API_KEY, FMXDNS_API_KEY

# Define DEFAULT_PROVIDER locally
DEFAULT_PROVIDER = "longcat" if LONGCAT_API_KEY else "fmxdns" if FMXDNS_API_KEY else "longcat"

from prefect_workflows.models import (
    SeedProduct,
    SeedProfile,
    MarketSynthesis,
    Candidate,
    Classification,
    CandidateCategory,
    SearchDirection,
    SourceType,
    SearchBreadth,
)


# ============================================================================
# SEED ANALYSIS TASKS
# ============================================================================

@task(
    name="analyze_seed_product",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(days=7),
    retries=2,
    retry_delay_seconds=5,
)
def analyze_seed_product(
    seed: SeedProduct,
    provider: str = DEFAULT_PROVIDER
) -> SeedProfile:
    """
    Analyze a seed product using LLM.
    
    Cached for 7 days - seeds don't change often.
    """
    logger = get_run_logger()
    logger.info(f"Analyzing seed: {seed.name}")
    
    # Build prompt
    prompt = f"""
Analyze this product and extract structured information:

Product Name: {seed.name}
{seed.website or ""}
{seed.notes or ""}

Extract:
1. Product category (e.g., "AI Code Assistant", "Project Management Tool")
2. Key features (list of 3-5 features)
3. Description (1-2 sentences)
4. How they position in the market
5. Target audience

Respond in this exact format:
Category: <category>
Features: <feature1>, <feature2>, <feature3>
Description: <description>
Positioning: <positioning>
Target: <target audience>
"""
    
    # Call LLM
    response = _call_llm(prompt, provider)
    
    # Parse response
    profile = _parse_seed_profile(response, seed)
    
    logger.info(f"Analyzed {seed.name}: {profile.category}")
    return profile


@task(
    name="synthesize_market_understanding",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def synthesize_market_understanding(
    seed_profiles: list[SeedProfile],
    provider: str = DEFAULT_PROVIDER
) -> MarketSynthesis:
    """
    Synthesize market understanding from seed profiles.
    """
    logger = get_run_logger()
    logger.info(f"Synthesizing market from {len(seed_profiles)} seeds")
    
    # Build context from seeds
    seeds_context = "\n\n".join([
        f"{p.name}: {p.category}\nFeatures: {', '.join(p.key_features)}\nPositioning: {p.positioning}"
        for p in seed_profiles
    ])
    
    prompt = f"""
Based on these seed products, synthesize the market landscape:

{seeds_context}

Provide:
1. Market category name (concise, 2-4 words)
2. Market description (1 paragraph)
3. Common features across products
4. Differentiating features
5. Typical customer profile
6. Common use cases
7. Relevant keywords for searching competitors
8. What this market is NOT (exclusions)

Format:
Category: <name>
Description: <description>
Common: <features>
Differentiators: <features>
Customer: <profile>
Use Cases: <cases>
Keywords: <keywords>
Not This: <exclusions>
"""
    
    response = _call_llm(prompt, provider)
    
    return _parse_market_synthesis(response)


# ============================================================================
# SEARCH DIRECTION GENERATION
# ============================================================================

@task(
    name="generate_search_directions",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def generate_search_directions(
    seed_profiles: list[SeedProfile],
    market_synthesis: MarketSynthesis,
    breadth: SearchBreadth,
    provider: str = DEFAULT_PROVIDER
) -> list[SearchDirection]:
    """
    Generate search queries to discover competitors.
    """
    logger = get_run_logger()
    logger.info(f"Generating search directions (breadth: {breadth.value})")
    
    # Determine number of queries based on breadth
    num_queries = {
        SearchBreadth.NARROW: 5,
        SearchBreadth.BALANCED: 10,
        SearchBreadth.BROAD: 20,
    }[breadth]
    
    seeds_context = "\n".join([f"- {p.name} ({p.category})" for p in seed_profiles])
    
    prompt = f"""
Generate {num_queries} search queries to discover competitors similar to these seeds:

Seeds:
{seeds_context}

Market: {market_synthesis.market_category}
Keywords: {', '.join(market_synthesis.relevant_keywords)}

Create search queries that would find:
- Directory listings (G2, Capterra)
- Comparison articles ("X vs Y")
- Alternative recommendations
- Review sites
- Community discussions

For each query, specify:
1. The exact search query
2. What type of results it targets (directory/comparison/review/alternative/media)
3. Priority (1-10, higher = more likely to find direct competitors)
4. Brief rationale

Format each as:
Query: <query>
Type: <type>
Priority: <number>
Rationale: <why>

---
"""
    
    response = _call_llm(prompt, provider)
    
    return _parse_search_directions(response)


# ============================================================================
# CLASSIFICATION TASKS
# ============================================================================

@task(
    name="classify_candidate",
    cache_policy=task_input_hash,
    cache_expiration=timedelta(days=30),
    retries=2,
)
def classify_candidate(
    candidate: Candidate,
    seed_profiles: list[SeedProfile],
    provider: str = DEFAULT_PROVIDER
) -> Classification:
    """
    Classify a candidate as direct/indirect/adjacent/not_relevant.
    
    Cached for 30 days - classification is stable.
    """
    logger = get_run_logger()
    logger.info(f"Classifying: {candidate.name}")
    
    # Build context
    seeds_context = "\n".join([
        f"- {p.name}: {', '.join(p.key_features)}"
        for p in seed_profiles
    ])
    
    candidate_features = ", ".join(candidate.key_features) if candidate.key_features else "Unknown"
    
    prompt = f"""
Classify this product relative to the seed products:

Candidate: {candidate.name}
Description: {candidate.description}
Features: {candidate_features}

Seeds (market reference):
{seeds_context}

Classify as one of:
- DIRECT: Same category, same core features, clear competitor
- INDIRECT: Overlapping use cases but different positioning
- ADJACENT: Related but different market/category
- NOT_RELEVANT: Doesn't fit this market

Provide:
1. Classification
2. Confidence (0.0-1.0)
3. Reasoning (1-2 sentences)
4. Matching features (if direct/indirect)
5. Contrasting features (if indirect/adjacent)

Format:
Category: <DIRECT/INDIRECT/ADJACENT/NOT_RELEVANT>
Confidence: <0.0-1.0>
Reasoning: <reasoning>
Matching: <features>
Contrasting: <features>
"""
    
    response = _call_llm(prompt, provider)
    
    return _parse_classification(response, candidate)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _call_llm(prompt: str, provider: str = DEFAULT_PROVIDER) -> str:
    """Call the appropriate LLM provider."""
    if provider == "openai":
        return _openai_complete(prompt)
    elif provider == "groq":
        return _groq_complete(prompt)
    else:  # ollama / longcat / fmxdns
        return _ollama_complete(prompt, provider)


def _parse_seed_profile(response: str, seed: SeedProduct) -> SeedProfile:
    """Parse LLM response into SeedProfile."""
    lines = response.strip().split("\n")
    
    data = {
        "category": "",
        "key_features": [],
        "description": "",
        "positioning": "",
        "target_audience": "",
    }
    
    for line in lines:
        line = line.strip()
        if line.startswith("Category:"):
            data["category"] = line.replace("Category:", "").strip()
        elif line.startswith("Features:"):
            features_text = line.replace("Features:", "").strip()
            data["key_features"] = [f.strip() for f in features_text.split(",")]
        elif line.startswith("Description:"):
            data["description"] = line.replace("Description:", "").strip()
        elif line.startswith("Positioning:"):
            data["positioning"] = line.replace("Positioning:", "").strip()
        elif line.startswith("Target:"):
            data["target_audience"] = line.replace("Target:", "").strip()
    
    return SeedProfile(
        name=seed.name,
        category=data["category"],
        key_features=data["key_features"],
        description=data["description"],
        website=seed.website or "",
        positioning=data["positioning"],
        target_audience=data["target_audience"],
    )


def _parse_market_synthesis(response: str) -> MarketSynthesis:
    """Parse LLM response into MarketSynthesis."""
    lines = response.strip().split("\n")
    
    data = {
        "market_category": "",
        "market_description": "",
        "common_features": [],
        "differentiating_features": [],
        "typical_customer": "",
        "use_cases": [],
        "relevant_keywords": [],
        "what_this_is_not": [],
    }
    
    current_key = None
    
    for line in lines:
        line = line.strip()
        if line.startswith("Category:"):
            data["market_category"] = line.replace("Category:", "").strip()
        elif line.startswith("Description:"):
            data["market_description"] = line.replace("Description:", "").strip()
        elif line.startswith("Common:"):
            features = line.replace("Common:", "").strip()
            data["common_features"] = [f.strip() for f in features.split(",")]
        elif line.startswith("Differentiators:"):
            features = line.replace("Differentiators:", "").strip()
            data["differentiating_features"] = [f.strip() for f in features.split(",")]
        elif line.startswith("Customer:"):
            data["typical_customer"] = line.replace("Customer:", "").strip()
        elif line.startswith("Use Cases:"):
            cases = line.replace("Use Cases:", "").strip()
            data["use_cases"] = [c.strip() for c in cases.split(",")]
        elif line.startswith("Keywords:"):
            keywords = line.replace("Keywords:", "").strip()
            data["relevant_keywords"] = [k.strip() for k in keywords.split(",")]
        elif line.startswith("Not This:"):
            exclusions = line.replace("Not This:", "").strip()
            data["what_this_is_not"] = [e.strip() for e in exclusions.split(",")]
    
    return MarketSynthesis(
        market_category=data["market_category"],
        market_description=data["market_description"],
        common_features=data["common_features"],
        differentiating_features=data["differentiating_features"],
        typical_customer=data["typical_customer"],
        use_cases=data["use_cases"],
        relevant_keywords=data["relevant_keywords"],
        what_this_is_not=data["what_this_is_not"],
    )


def _parse_search_directions(response: str) -> list[SearchDirection]:
    """Parse LLM response into list of SearchDirection."""
    directions = []
    
    # Split by --- or blank lines to get individual directions
    sections = response.split("---")
    
    for section in sections:
        lines = section.strip().split("\n")
        
        query = ""
        source_type = SourceType.MEDIA
        priority = 5
        rationale = ""
        
        for line in lines:
            line = line.strip()
            if line.startswith("Query:"):
                query = line.replace("Query:", "").strip()
            elif line.startswith("Type:"):
                type_str = line.replace("Type:", "").strip().lower()
                if "directory" in type_str:
                    source_type = SourceType.DIRECTORY
                elif "comparison" in type_str:
                    source_type = SourceType.COMPARISON
                elif "review" in type_str:
                    source_type = SourceType.REVIEW
                elif "alternative" in type_str:
                    source_type = SourceType.ALTERNATIVE
            elif line.startswith("Priority:"):
                try:
                    priority = int(line.replace("Priority:", "").strip())
                except ValueError:
                    priority = 5
            elif line.startswith("Rationale:"):
                rationale = line.replace("Rationale:", "").strip()
        
        if query:
            directions.append(SearchDirection(
                query=query,
                source_type=source_type,
                priority=priority,
                rationale=rationale
            ))
    
    return directions


def _parse_classification(response: str, candidate: Candidate) -> Classification:
    """Parse LLM response into Classification."""
    lines = response.strip().split("\n")
    
    category = CandidateCategory.NOT_RELEVANT
    confidence = 0.5
    reasoning = ""
    matching = []
    contrasting = []
    
    for line in lines:
        line = line.strip()
        if line.startswith("Category:"):
            cat_str = line.replace("Category:", "").strip().upper()
            if "DIRECT" in cat_str:
                category = CandidateCategory.DIRECT
            elif "INDIRECT" in cat_str:
                category = CandidateCategory.INDIRECT
            elif "ADJACENT" in cat_str:
                category = CandidateCategory.ADJACENT
        elif line.startswith("Confidence:"):
            try:
                confidence = float(line.replace("Confidence:", "").strip())
            except ValueError:
                confidence = 0.5
        elif line.startswith("Reasoning:"):
            reasoning = line.replace("Reasoning:", "").strip()
        elif line.startswith("Matching:"):
            features = line.replace("Matching:", "").strip()
            matching = [f.strip() for f in features.split(",") if f.strip()]
        elif line.startswith("Contrasting:"):
            features = line.replace("Contrasting:", "").strip()
            contrasting = [f.strip() for f in features.split(",") if f.strip()]
    
    return Classification(
        candidate_id=candidate.id,
        candidate_name=candidate.name,
        category=category,
        confidence=confidence,
        reasoning=reasoning,
        evidence_features=matching,
        contrasting_features=contrasting,
    )
