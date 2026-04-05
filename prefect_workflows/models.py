"""
Prefect Workflows - Data Models

All Pydantic models for the multi-seed market discovery system.
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field
from uuid import uuid4


# ============================================================================
# ENUMS
# ============================================================================

class SearchBreadth(str, Enum):
    NARROW = "narrow"      # Conservative, high confidence only
    BALANCED = "balanced"  # Default
    BROAD = "broad"        # Aggressive exploration


class CandidateCategory(str, Enum):
    DIRECT = "direct"           # Same category, same features
    INDIRECT = "indirect"       # Overlapping use cases
    ADJACENT = "adjacent"       # Related but different market
    NOT_RELEVANT = "not_relevant"  # Doesn't fit scope


class SourceType(str, Enum):
    DIRECTORY = "directory"       # G2, Capterra, etc.
    REVIEW = "review"              # Trustpilot, Reddit reviews
    COMPARISON = "comparison"      # "X vs Y" articles
    ALTERNATIVE = "alternative"    # AlternativeTo, etc.
    MEDIA = "media"                # Tech news, press releases
    COMMUNITY = "community"        # Forums, Reddit discussions
    NEWS = "news"                  # General news coverage


class DiscoveryPhase(str, Enum):
    SEED_ANALYSIS = "seed_analysis"
    SEARCH_DIRECTIONS = "search_directions"
    CANDIDATE_DISCOVERY = "candidate_discovery"
    CLASSIFICATION = "classification"
    SOURCE_DISCOVERY = "source_discovery"
    COMPLETE = "complete"


# ============================================================================
# INPUTS
# ============================================================================

class SeedProduct(BaseModel):
    """A known product to start the discovery from."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    website: Optional[str] = None
    notes: Optional[str] = None
    
    class Config:
        frozen = True  # Immutable for caching


class ScopeConfig(BaseModel):
    """User-defined scope for the market discovery."""
    target_market: Optional[str] = None
    customer_type: Optional[str] = None
    product_category: Optional[str] = None
    must_have_features: list[str] = Field(default_factory=list)
    exclusions: list[str] = Field(default_factory=list)
    
    class Config:
        frozen = True


class ResultLimits(BaseModel):
    """Limits to control discovery breadth."""
    max_candidates: int = 50
    max_sources_per_candidate: int = 10
    max_total_sources: int = 100
    early_stop_on_strong_results: bool = True
    
    class Config:
        frozen = True


class DiscoveryRequest(BaseModel):
    """Complete request to start a market discovery."""
    seed_products: list[SeedProduct]
    scope_config: ScopeConfig = Field(default_factory=ScopeConfig)
    breadth: SearchBreadth = SearchBreadth.BALANCED
    limits: ResultLimits = Field(default_factory=ResultLimits)


# ============================================================================
# INTERMEDIATE DATA
# ============================================================================

class SeedProfile(BaseModel):
    """Enriched profile of a seed product after analysis."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    category: str
    key_features: list[str] = Field(default_factory=list)
    description: str = ""
    website: str = ""
    positioning: str = ""  # How they position in market
    target_audience: str = ""
    analysis_timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Source of information
    evidence_urls: list[str] = Field(default_factory=list)
    
    class Config:
        frozen = True


class MarketSynthesis(BaseModel):
    """High-level understanding derived from seed analysis."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    
    # Market boundaries
    market_category: str = ""
    market_description: str = ""
    
    # Common patterns
    common_features: list[str] = Field(default_factory=list)
    differentiating_features: list[str] = Field(default_factory=list)
    
    # Target customer
    typical_customer: str = ""
    use_cases: list[str] = Field(default_factory=list)
    
    # Discovery hints
    relevant_keywords: list[str] = Field(default_factory=list)
    search_suggestions: list[str] = Field(default_factory=list)
    
    # Exclusions derived from seeds
    what_this_is_not: list[str] = Field(default_factory=list)


class SearchDirection(BaseModel):
    """A generated search query with metadata."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    query: str
    source_type: SourceType
    priority: int = 5  # 1-10, higher = more important
    rationale: str = ""  # Why this query was generated
    generated_from_seed: Optional[str] = None  # Which seed inspired this


class RawCandidate(BaseModel):
    """A candidate discovered from a search (before deduplication)."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    discovery_source: str  # Search query that found this
    evidence_url: str
    evidence_snippet: str
    confidence_score: float = 0.5
    search_direction_id: Optional[str] = None


class Candidate(BaseModel):
    """A deduplicated and enriched product candidate."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    website: Optional[str] = None
    description: str = ""
    
    # Discovery provenance
    discovery_sources: list[str] = Field(default_factory=list)  # All search queries
    evidence_links: list[str] = Field(default_factory=list)
    first_discovered_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Enrichment data
    key_features: list[str] = Field(default_factory=list)
    pricing_hint: Optional[str] = None
    target_audience_hint: Optional[str] = None
    
    # User corrections
    user_notes: Optional[str] = None
    user_removed: bool = False  # User marked as not relevant


class Classification(BaseModel):
    """AI classification of a candidate's relationship to the market."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    candidate_id: str
    candidate_name: str  # Denormalized for UI display
    
    # Classification
    category: CandidateCategory
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""
    
    # Evidence
    evidence_features: list[str] = Field(default_factory=list)
    contrasting_features: list[str] = Field(default_factory=list)  # Why not direct
    
    # User review
    user_reviewed: bool = False
    user_approved: bool = False
    user_modified_category: Optional[CandidateCategory] = None
    user_feedback: Optional[str] = None


class ThirdPartySource(BaseModel):
    """A third-party site where a candidate appears."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    candidate_id: str
    candidate_name: str  # Denormalized
    
    url: str
    site_name: str  # "G2", "Capterra", etc.
    site_type: SourceType
    
    title: str = ""
    snippet: str = ""
    relevance_score: float = 0.5
    
    # For lead discovery
    is_discovery_site: bool = False  # Can this site find MORE candidates?


class LeadDiscoverySite(BaseModel):
    """A site useful for finding more leads/competitors."""
    site_name: str
    url_pattern: str  # e.g., "https://www.g2.com/products/[category]"
    site_type: SourceType
    
    candidates_found_count: int = 0
    unique_candidates: list[str] = Field(default_factory=list)
    
    usefulness_score: float = 0.0  # Calculated based on coverage
    notes: str = ""


# ============================================================================
# CHECKPOINTS & STATE
# ============================================================================

class CheckpointData(BaseModel):
    """Data saved at a checkpoint for review/resume."""
    phase: DiscoveryPhase
    phase_number: int  # 1-5
    
    # Data available at this checkpoint
    seed_profiles: Optional[list[SeedProfile]] = None
    search_directions: Optional[list[SearchDirection]] = None
    discovered_candidates: Optional[list[Candidate]] = None
    classifications: Optional[list[Classification]] = None
    approved_candidates: Optional[list[Candidate]] = None
    third_party_sources: Optional[list[ThirdPartySource]] = None
    lead_sites: Optional[list[LeadDiscoverySite]] = None
    
    # Metadata
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    item_count: int = 0  # Number of items for review


class UserReviewResult(BaseModel):
    """User's corrections/approval at a checkpoint."""
    checkpoint_id: str
    approved: bool = False
    modified: bool = False
    
    # Corrections by entity type
    removed_seed_ids: list[str] = Field(default_factory=list)
    modified_search_directions: Optional[list[SearchDirection]] = None
    removed_candidate_ids: list[str] = Field(default_factory=list)
    modified_classifications: Optional[list[Classification]] = None  # User reclassified
    added_candidates_manually: list[Candidate] = Field(default_factory=list)
    removed_source_ids: list[str] = Field(default_factory=list)
    
    # Global actions
    notes: Optional[str] = None
    stop_here: bool = False  # User wants to stop, don't continue


class FlowState(BaseModel):
    """Complete state of a discovery flow for persistence."""
    flow_id: str
    status: Literal["running", "paused", "completed", "failed"] = "running"
    current_phase: DiscoveryPhase = DiscoveryPhase.SEED_ANALYSIS
    
    # Inputs (immutable after start)
    seed_products: list[SeedProduct]
    scope_config: ScopeConfig
    breadth: SearchBreadth
    limits: ResultLimits
    
    # Collected results (grows as flow progresses)
    seed_profiles: list[SeedProfile] = Field(default_factory=list)
    market_synthesis: Optional[MarketSynthesis] = None
    search_directions: list[SearchDirection] = Field(default_factory=list)
    discovered_candidates: list[Candidate] = Field(default_factory=list)
    classifications: list[Classification] = Field(default_factory=list)
    approved_candidates: list[Candidate] = Field(default_factory=list)
    third_party_sources: list[ThirdPartySource] = Field(default_factory=list)
    lead_sites: list[LeadDiscoverySite] = Field(default_factory=list)
    
    # Checkpoints
    checkpoints: list[CheckpointData] = Field(default_factory=list)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None


# ============================================================================
# OUTPUTS
# ============================================================================

class MarketStatistics(BaseModel):
    """Statistics about the discovered market."""
    total_candidates: int = 0
    direct_competitors: int = 0
    indirect_competitors: int = 0
    adjacent_tools: int = 0
    not_relevant: int = 0
    
    total_sources_found: int = 0
    unique_discovery_sites: int = 0
    
    # Coverage analysis
    seeds_with_direct_competitors: int = 0
    avg_sources_per_candidate: float = 0.0


class MarketMap(BaseModel):
    """Final market map output."""
    direct_competitors: list[Candidate]
    indirect_competitors: list[Candidate]
    adjacent_tools: list[Candidate]
    not_relevant: list[Candidate] = Field(default_factory=list)  # Kept for audit
    
    statistics: MarketStatistics
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Summary
    market_category: str = ""
    key_insights: list[str] = Field(default_factory=list)


class AuditEntry(BaseModel):
    """Single audit log entry."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    action: str  # "discovered", "classified", "user_removed", "user_reclassified", etc.
    entity_type: str  # "candidate", "classification", "source"
    entity_id: str
    entity_name: str  # Human-readable
    details: dict[str, Any] = Field(default_factory=dict)
    user_initiated: bool = False  # True if from user action


class AuditTrail(BaseModel):
    """Complete audit trail of the discovery process."""
    flow_id: str
    entries: list[AuditEntry] = Field(default_factory=list)
    
    # Summary
    total_api_calls: int = 0
    total_llm_calls: int = 0
    total_crawls: int = 0
    user_interventions: int = 0


class ExportBundle(BaseModel):
    """Multiple export formats."""
    json_path: Optional[str] = None
    excel_path: Optional[str] = None
    markdown_report: Optional[str] = None


class DiscoveryResult(BaseModel):
    """Complete result of a market discovery flow."""
    flow_id: str
    
    market_map: MarketMap
    lead_dataset: list[LeadDiscoverySite]
    audit_trail: AuditTrail
    export_formats: ExportBundle
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    duration_seconds: float = 0.0
    checkpoint_reached: int = 0  # Which checkpoint was last


# ============================================================================
# EVENTS (For Real-time Frontend Updates)
# ============================================================================

class FlowEvent(BaseModel):
    """Event emitted during flow execution."""
    flow_id: str
    event_type: str  # "phase.started", "task.completed", "checkpoint.reached", etc.
    phase: Optional[DiscoveryPhase] = None
    
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TaskProgress(BaseModel):
    """Progress update for a specific task."""
    task_name: str
    current: int
    total: int
    message: str = ""
    
    @property
    def percentage(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.current / self.total) * 100
