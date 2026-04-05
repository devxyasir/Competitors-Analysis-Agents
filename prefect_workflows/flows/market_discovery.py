"""
Market Discovery Flow - Main Prefect Flow

Multi-seed market discovery with 5 human-in-the-loop checkpoints.
"""

import asyncio
from datetime import datetime
from typing import Optional
from uuid import uuid4

from prefect import flow, task, get_run_logger, pause_flow_run
from prefect.flow_runs import pause_flow_run as prefect_pause
from prefect.input import run_input

from prefect_workflows.models import (
    DiscoveryRequest,
    SeedProduct,
    SeedProfile,
    MarketSynthesis,
    SearchDirection,
    Candidate,
    Classification,
    ThirdPartySource,
    LeadDiscoverySite,
    DiscoveryResult,
    MarketMap,
    MarketStatistics,
    AuditTrail,
    AuditEntry,
    FlowState,
    CheckpointData,
    UserReviewResult,
    DiscoveryPhase,
    ExportBundle,
    TaskProgress,
)

from prefect_workflows.infrastructure.checkpoint_manager import get_checkpoint_manager
from prefect_workflows.infrastructure.event_emitter import create_emitter

# Import tasks
from prefect_workflows.tasks.search_tasks import (
    execute_discovery_search,
    execute_source_search,
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


# ============================================================================
# MAIN FLOW
# ============================================================================

@flow(
    name="market_discovery",
    description="Multi-seed market discovery with human-in-the-loop checkpoints",
    log_prints=True,
)
async def market_discovery_flow(
    seed_products: list[SeedProduct],
    scope_config: Optional[dict] = None,
    breadth: str = "balanced",
    limits: Optional[dict] = None,
    flow_id: Optional[str] = None,
    resume_from_checkpoint: Optional[str] = None,
) -> DiscoveryResult:
    """
    Main market discovery flow with 5 checkpoints for human review.
    
    Checkpoints:
    1. Seed Analysis - Review analyzed seed profiles
    2. Search Directions - Review generated search queries
    3. Discovered Candidates - Review raw candidates
    4. Classification - CRITICAL: Review and correct classifications
    5. Sources - Review discovered third-party sites
    
    Args:
        seed_products: List of known products to start from
        scope_config: Market scope definition
        breadth: "narrow", "balanced", or "broad"
        limits: Result limits
        flow_id: Optional flow ID (generated if not provided)
        resume_from_checkpoint: Checkpoint ID to resume from
    
    Returns:
        DiscoveryResult with market map, lead dataset, and audit trail
    """
    # Initialize
    flow_id = flow_id or str(uuid4())
    logger = get_run_logger()
    logger.info(f"Starting market discovery flow: {flow_id}")
    
    # Parse inputs
    from prefect_workflows.models import ScopeConfig, ResultLimits, SearchBreadth
    scope = ScopeConfig(**(scope_config or {}))
    breadth_enum = SearchBreadth(breadth)
    limits_obj = ResultLimits(**(limits or {}))
    
    # Initialize infrastructure
    emitter = create_emitter(flow_id)
    checkpoint_mgr = get_checkpoint_manager()
    
    # Create or load flow state
    if resume_from_checkpoint:
        state = checkpoint_mgr.load_flow_state(flow_id)
        if not state:
            raise ValueError(f"Cannot resume: no state found for {flow_id}")
        logger.info(f"Resuming from checkpoint: {resume_from_checkpoint}")
    else:
        state = FlowState(
            flow_id=flow_id,
            seed_products=seed_products,
            scope_config=scope,
            breadth=breadth_enum,
            limits=limits_obj,
        )
    
    # =========================================================================
    # PHASE 1: SEED ANALYSIS
    # =========================================================================
    
    if not resume_from_checkpoint or state.current_phase == DiscoveryPhase.SEED_ANALYSIS:
        emitter.phase_started(DiscoveryPhase.SEED_ANALYSIS, "Analyzing seed products")
        
        # Analyze seeds in parallel
        logger.info(f"Analyzing {len(seed_products)} seed products")
        futures = analyze_seed_product.map(seed_products)
        seed_profiles = await asyncio.gather(*futures)
        state.seed_profiles = list(seed_profiles)
        
        # Synthesize market understanding
        state.market_synthesis = synthesize_market_understanding(state.seed_profiles)
        
        state.current_phase = DiscoveryPhase.SEARCH_DIRECTIONS
        checkpoint_mgr.save_flow_state(state)
        
        emitter.phase_completed(DiscoveryPhase.SEED_ANALYSIS, len(state.seed_profiles))
        
        # CHECKPOINT 1: Seed Analysis Review
        checkpoint_data = CheckpointData(
            phase=DiscoveryPhase.SEED_ANALYSIS,
            phase_number=1,
            seed_profiles=state.seed_profiles,
            item_count=len(state.seed_profiles),
        )
        checkpoint_id = checkpoint_mgr.save_checkpoint(flow_id, checkpoint_data, state)
        
        emitter.checkpoint_reached(
            DiscoveryPhase.SEED_ANALYSIS,
            len(state.seed_profiles),
            checkpoint_id
        )
        
        # Pause for user review (optional - can be configured to skip)
        # For now, auto-continue but save checkpoint
    
    # =========================================================================
    # PHASE 2: SEARCH DIRECTION GENERATION
    # =========================================================================
    
    if state.current_phase == DiscoveryPhase.SEARCH_DIRECTIONS:
        emitter.phase_started(DiscoveryPhase.SEARCH_DIRECTIONS, "Generating search directions")
        
        # Generate search queries
        state.search_directions = generate_search_directions(
            state.seed_products,  # Use original seeds for context
            state.market_synthesis,
            state.breadth,
        )
        
        state.current_phase = DiscoveryPhase.CANDIDATE_DISCOVERY
        checkpoint_mgr.save_flow_state(state)
        
        emitter.phase_completed(DiscoveryPhase.SEARCH_DIRECTIONS, len(state.search_directions))
        
        # CHECKPOINT 2: Search Directions Review
        checkpoint_data = CheckpointData(
            phase=DiscoveryPhase.SEARCH_DIRECTIONS,
            phase_number=2,
            seed_profiles=state.seed_profiles,
            search_directions=state.search_directions,
            item_count=len(state.search_directions),
        )
        checkpoint_id = checkpoint_mgr.save_checkpoint(flow_id, checkpoint_data, state)
        
        emitter.checkpoint_reached(
            DiscoveryPhase.SEARCH_DIRECTIONS,
            len(state.search_directions),
            checkpoint_id
        )
    
    # =========================================================================
    # PHASE 3: CANDIDATE DISCOVERY
    # =========================================================================
    
    if state.current_phase == DiscoveryPhase.CANDIDATE_DISCOVERY:
        emitter.phase_started(DiscoveryPhase.CANDIDATE_DISCOVERY, "Discovering candidates")
        
        # Execute searches in parallel
        logger.info(f"Executing {len(state.search_directions)} search directions")
        raw_results = await execute_discovery_search.map(state.search_directions)
        
        # Deduplicate and enrich
        seed_names = [s.name for s in state.seed_products]
        deduplicated = deduplicate_candidates(raw_results, exclusion_list=seed_names)
        
        # Limit before enrichment
        if len(deduplicated) > state.limits.max_candidates:
            logger.info(f"Limiting to top {state.limits.max_candidates} candidates")
            deduplicated = deduplicated[:state.limits.max_candidates]
        
        # Enrich candidates
        state.discovered_candidates = await enrich_candidates(
            deduplicated,
            state.limits.max_candidates
        )
        
        state.current_phase = DiscoveryPhase.CLASSIFICATION
        checkpoint_mgr.save_flow_state(state)
        
        emitter.phase_completed(DiscoveryPhase.CANDIDATE_DISCOVERY, len(state.discovered_candidates))
        
        # CHECKPOINT 3: Discovered Candidates Review
        checkpoint_data = CheckpointData(
            phase=DiscoveryPhase.CANDIDATE_DISCOVERY,
            phase_number=3,
            seed_profiles=state.seed_profiles,
            search_directions=state.search_directions,
            discovered_candidates=state.discovered_candidates,
            item_count=len(state.discovered_candidates),
        )
        checkpoint_id = checkpoint_mgr.save_checkpoint(flow_id, checkpoint_data, state)
        
        emitter.checkpoint_reached(
            DiscoveryPhase.CANDIDATE_DISCOVERY,
            len(state.discovered_candidates),
            checkpoint_id
        )
    
    # =========================================================================
    # PHASE 4: CANDIDATE CLASSIFICATION
    # =========================================================================
    
    if state.current_phase == DiscoveryPhase.CLASSIFICATION:
        emitter.phase_started(DiscoveryPhase.CLASSIFICATION, "Classifying candidates")
        
        # Classify in parallel
        logger.info(f"Classifying {len(state.discovered_candidates)} candidates")
        classifications = await classify_candidate.map(
            state.discovered_candidates,
            state.seed_profiles,
        )
        state.classifications = list(classifications)
        
        state.current_phase = DiscoveryPhase.SOURCE_DISCOVERY
        checkpoint_mgr.save_flow_state(state)
        
        emitter.phase_completed(DiscoveryPhase.CLASSIFICATION, len(state.classifications))
        
        # CHECKPOINT 4: Classification Review (CRITICAL)
        checkpoint_data = CheckpointData(
            phase=DiscoveryPhase.CLASSIFICATION,
            phase_number=4,
            seed_profiles=state.seed_profiles,
            search_directions=state.search_directions,
            discovered_candidates=state.discovered_candidates,
            classifications=state.classifications,
            item_count=len(state.classifications),
        )
        checkpoint_id = checkpoint_mgr.save_checkpoint(flow_id, checkpoint_data, state)
        
        emitter.checkpoint_reached(
            DiscoveryPhase.CLASSIFICATION,
            len(state.classifications),
            checkpoint_id
        )
        
        # For now, filter without user review (can add pause here later)
        state.approved_candidates = filter_approved_candidates(
            state.discovered_candidates,
            state.classifications,
        )
    
    # =========================================================================
    # PHASE 5: THIRD-PARTY SOURCE DISCOVERY
    # =========================================================================
    
    if state.current_phase == DiscoveryPhase.SOURCE_DISCOVERY:
        emitter.phase_started(DiscoveryPhase.SOURCE_DISCOVERY, "Discovering third-party sources")
        
        # Discover sources for approved candidates
        logger.info(f"Finding sources for {len(state.approved_candidates)} candidates")
        
        # Build search queries for each candidate
        source_searches = []
        for candidate in state.approved_candidates:
            # Search on major sites
            queries = [
                f'"{candidate.name}" site:g2.com',
                f'"{candidate.name}" site:capterra.com',
                f'"{candidate.name}" site:alternativeto.net',
                f'"{candidate.name}" site:reddit.com',
                f'"{candidate.name}" reviews',
            ]
            for query in queries:
                source_searches.append((candidate, query))
        
        # Limit total searches
        if len(source_searches) > state.limits.max_total_sources:
            source_searches = source_searches[:state.limits.max_total_sources]
        
        # Execute in parallel
        source_results = []
        for candidate, query in source_searches:
            result = await execute_source_search(candidate.name, query)
            # Tag with candidate ID
            for source in result:
                source.candidate_id = candidate.id
            source_results.append(result)
        
        # Flatten
        state.third_party_sources = []
        for batch in source_results:
            state.third_party_sources.extend(batch)
        
        # Rank discovery sites
        state.lead_sites = rank_source_sites(source_results)
        
        state.current_phase = DiscoveryPhase.COMPLETE
        checkpoint_mgr.save_flow_state(state)
        
        emitter.phase_completed(DiscoveryPhase.SOURCE_DISCOVERY, len(state.third_party_sources))
        
        # CHECKPOINT 5: Sources Review (Optional)
        checkpoint_data = CheckpointData(
            phase=DiscoveryPhase.SOURCE_DISCOVERY,
            phase_number=5,
            seed_profiles=state.seed_profiles,
            search_directions=state.search_directions,
            discovered_candidates=state.discovered_candidates,
            classifications=state.classifications,
            approved_candidates=state.approved_candidates,
            third_party_sources=state.third_party_sources,
            lead_sites=state.lead_sites,
            item_count=len(state.lead_sites),
        )
        checkpoint_id = checkpoint_mgr.save_checkpoint(flow_id, checkpoint_data, state)
        
        emitter.checkpoint_reached(
            DiscoveryPhase.SOURCE_DISCOVERY,
            len(state.lead_sites),
            checkpoint_id
        )
    
    # =========================================================================
    # OUTPUT GENERATION
    # =========================================================================
    
    logger.info("Generating final output")
    
    # Build market map
    from prefect_workflows.models import CandidateCategory
    
    direct = [c for c in state.approved_candidates if any(
        cl.candidate_id == c.id and cl.category == CandidateCategory.DIRECT
        for cl in state.classifications
    )]
    indirect = [c for c in state.approved_candidates if any(
        cl.candidate_id == c.id and cl.category == CandidateCategory.INDIRECT
        for cl in state.classifications
    )]
    adjacent = [c for c in state.approved_candidates if any(
        cl.candidate_id == c.id and cl.category == CandidateCategory.ADJACENT
        for cl in state.classifications
    )]
    
    stats = MarketStatistics(
        total_candidates=len(state.discovered_candidates),
        direct_competitors=len(direct),
        indirect_competitors=len(indirect),
        adjacent_tools=len(adjacent),
        not_relevant=len(state.discovered_candidates) - len(state.approved_candidates),
        total_sources_found=len(state.third_party_sources),
        unique_discovery_sites=len(state.lead_sites),
    )
    
    market_map = MarketMap(
        direct_competitors=direct,
        indirect_competitors=indirect,
        adjacent_tools=adjacent,
        statistics=stats,
        market_category=state.market_synthesis.market_category if state.market_synthesis else "",
        key_insights=state.market_synthesis.search_suggestions if state.market_synthesis else [],
    )
    
    # Build audit trail
    audit = AuditTrail(
        flow_id=flow_id,
        entries=[],  # Would populate from actual task runs
        total_api_calls=len(state.search_directions) + len(state.third_party_sources),
        total_llm_calls=len(state.seed_profiles) + len(state.discovered_candidates),
        total_crawls=len(state.approved_candidates),
    )
    
    # Create exports
    exports = ExportBundle()  # Would generate actual files
    
    result = DiscoveryResult(
        flow_id=flow_id,
        market_map=market_map,
        lead_dataset=state.lead_sites,
        audit_trail=audit,
        export_formats=exports,
        duration_seconds=(datetime.utcnow() - state.created_at).total_seconds(),
        checkpoint_reached=5,
    )
    
    emitter.flow_completed({
        "direct_competitors": len(direct),
        "indirect_competitors": len(indirect),
        "adjacent_tools": len(adjacent),
        "total_sources": len(state.third_party_sources),
    })
    
    logger.info(f"Flow completed: {flow_id}")
    return result
