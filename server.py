import asyncio
import io
import json
import os
from datetime import datetime
from contextvars import ContextVar
from typing import List, Dict, Optional, Any
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from orchestrator import discover_and_analyze, run_all
from models import CompetitorProfile
from database import save_analysis, get_history_list, get_analysis_by_id

# Import Prefect workflows
from prefect_workflows.flows.market_discovery import market_discovery_flow
from prefect_workflows.models import SeedProduct, ScopeConfig, ResultLimits, SearchBreadth
from prefect_workflows.infrastructure.checkpoint_manager import get_checkpoint_manager

# Context variable to hold a queue for SSE log streaming
log_queue_var: ContextVar[Optional[asyncio.Queue]] = ContextVar("log_queue", default=None)

# We will patch built-in print to also send to the queue if it's set
import builtins
_original_print = builtins.print

def patched_print(*args, **kwargs):
    _original_print(*args, **kwargs)
    q = log_queue_var.get()
    if q is not None:
        # Avoid blocking, print is synchronous
        # Try putting into queue, but if it's full/failed, just skip
        sep = kwargs.get("sep", " ")
        message = sep.join(str(a) for a in args)
        try:
            q.put_nowait(message)
        except asyncio.QueueFull:
            pass

builtins.print = patched_print

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For local Vite dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalyzeRequest(BaseModel):
    competitors: List[Dict[str, str]]
    timeout: int = 50
    maxResults: int = 100
    provider: str = "longcat"

class DiscoverRequest(BaseModel):
    target_name: str
    target_domain: str
    timeout: int = 50
    maxResults: int = 10
    provider: str = "longcat"

# ============================================================================
# MODULE 1: Analyze provided list of competitors (ORIGINAL)
# ============================================================================

@app.post("/api/analyze")
async def analyze_endpoint(request: AnalyzeRequest, req: Request):
    """Original endpoint: Analyze a provided list of competitors."""
    q = asyncio.Queue()
    
    async def sse_generator():
        log_queue_var.set(q)
        
        # Configure search parameters
        import config
        import agents.brave_search
        config.COMPETITORS = request.competitors
        agents.brave_search.REQUEST_TIMEOUT = request.timeout
        agents.brave_search.RESULTS_PER_CALL = request.maxResults
        
        # Start the analysis
        analysis_task = asyncio.create_task(run_all(provider=request.provider))
        
        # Stream logs
        while not analysis_task.done():
            try:
                msg = await asyncio.wait_for(q.get(), timeout=0.5)
                yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"
            except asyncio.TimeoutError:
                if await req.is_disconnected():
                    analysis_task.cancel()
                    break
        
        if not analysis_task.cancelled():
            while not q.empty():
                yield f"data: {json.dumps({'type': 'log', 'message': q.get_nowait()})}\n\n"
                
            try:
                profiles, synthesis = await analysis_task
                profile_dicts = [p.model_dump() for p in profiles]
                
                await save_analysis(request.competitors, profile_dicts, synthesis, request.provider)
                
                yield f"data: {json.dumps({'type': 'result', 'data': profile_dicts, 'synthesis': synthesis})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                
        yield "data: [DONE]\n\n"

    return StreamingResponse(sse_generator(), media_type="text/event-stream")


# ============================================================================
# MODULE 2: Discover competitors from single target (NEW)
# ============================================================================

@app.post("/api/discover")
async def discover_endpoint(request: DiscoverRequest, req: Request):
    """New endpoint: Discover and analyze competitors of a single target."""
    q = asyncio.Queue()
    
    async def sse_generator():
        log_queue_var.set(q)
        
        # Configure search parameters
        import agents.brave_search
        agents.brave_search.REQUEST_TIMEOUT = request.timeout
        agents.brave_search.RESULTS_PER_CALL = request.maxResults
        
        # Start the discovery and analysis
        analysis_task = asyncio.create_task(
            discover_and_analyze(
                target_name=request.target_name,
                target_domain=request.target_domain,
                provider=request.provider,
                max_results=request.maxResults
            )
        )
        
        # Stream logs
        while not analysis_task.done():
            try:
                msg = await asyncio.wait_for(q.get(), timeout=0.5)
                yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"
            except asyncio.TimeoutError:
                if await req.is_disconnected():
                    analysis_task.cancel()
                    break
        
        if not analysis_task.cancelled():
            while not q.empty():
                yield f"data: {json.dumps({'type': 'log', 'message': q.get_nowait()})}\n\n"
                
            try:
                profiles, synthesis = await analysis_task
                profile_dicts = [p.model_dump() for p in profiles]
                
                # Save to MongoDB with the target as context
                await save_analysis(
                    [{"name": request.target_name, "domain": request.target_domain}],
                    profile_dicts,
                    synthesis,
                    request.provider
                )
                
                yield f"data: {json.dumps({'type': 'result', 'data': profile_dicts, 'synthesis': synthesis})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                
        yield "data: [DONE]\n\n"

    return StreamingResponse(sse_generator(), media_type="text/event-stream")


@app.get("/api/history")
async def history_list_endpoint():
    try:
        history = await get_history_list()
        return {"status": "success", "history": history}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/history/{run_id}")
async def history_detail_endpoint(run_id: str):
    try:
        data = await get_analysis_by_id(run_id)
        if not data:
            return {"status": "error", "message": "Analysis not found"}
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}


class ExportRequest(BaseModel):
    profiles: List[Any]
    target_name: Optional[str] = None

@app.post("/api/export")
async def export_endpoint(request: ExportRequest):
    """Export analysis to Excel with proper multi-sheet structure."""
    # Convert dicts back to CompetitorProfile objects
    profiles = [CompetitorProfile(**p) for p in request.profiles]
    
    # Generate Excel with multiple sheets using the improved writer
    from output.writer import export_excel
    import tempfile
    
    # Create temp file
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        tmp_path = tmp.name
    
    # Export to temp file
    export_excel(profiles, path=tmp_path, target_name=request.target_name)
    
    # Read the file and return as response
    with open(tmp_path, 'rb') as f:
        excel_bytes = f.read()
    
    # Clean up temp file
    os.unlink(tmp_path)
    
    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if request.target_name:
        safe_target = "".join(c if c.isalnum() else "_" for c in request.target_name[:30])
        filename = f"competitor_analysis_{safe_target}_{timestamp}.xlsx"
    else:
        filename = f"competitor_analysis_{timestamp}.xlsx"
    
    return Response(
        content=excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.post("/api/export/json")
async def export_json_endpoint(request: ExportRequest):
    """Export analysis to JSON format."""
    profiles = [CompetitorProfile(**p) for p in request.profiles]
    
    from output.writer import export_json
    import tempfile
    
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False, mode='w', encoding='utf-8') as tmp:
        tmp_path = tmp.name
    
    export_json(profiles, path=tmp_path, target_name=request.target_name)
    
    with open(tmp_path, 'r', encoding='utf-8') as f:
        json_content = f.read()
    
    os.unlink(tmp_path)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if request.target_name:
        safe_target = "".join(c if c.isalnum() else "_" for c in request.target_name[:30])
        filename = f"competitor_analysis_{safe_target}_{timestamp}.json"
    else:
        filename = f"competitor_analysis_{timestamp}.json"
    
    return Response(
        content=json_content,
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ============================================================================
# PREFECT WORKFLOW ENDPOINTS - Multi-Seed Market Discovery
# ============================================================================

class MarketDiscoveryRequest(BaseModel):
    """Request to start a market discovery workflow."""
    seed_products: List[Dict[str, Optional[str]]]  # name, website, notes
    scope_config: Optional[Dict[str, Any]] = None
    breadth: str = "balanced"  # narrow, balanced, broad
    limits: Optional[Dict[str, int]] = None


class CheckpointReviewRequest(BaseModel):
    """User's review/corrections at a checkpoint."""
    flow_id: str
    phase: str
    approved: bool = True
    modifications: Optional[Dict[str, Any]] = None


@app.post("/api/discovery/market/start")
async def start_market_discovery(request: MarketDiscoveryRequest):
    """
    Start a new multi-seed market discovery workflow.
    
    This is the main endpoint for the new Prefect-based discovery system.
    """
    try:
        # Parse seed products
        seeds = [SeedProduct(**s) for s in request.seed_products]
        
        # Parse optional configs
        scope = ScopeConfig(**(request.scope_config or {}))
        limits = ResultLimits(**(request.limits or {}))
        breadth = SearchBreadth(request.breadth)
        
        # Submit flow to Prefect (async background execution)
        flow_run = await market_discovery_flow.submit(
            seed_products=seeds,
            scope_config=scope.model_dump(),
            breadth=breadth.value,
            limits=limits.model_dump(),
        )
        
        return {
            "status": "started",
            "flow_id": flow_run.flow_run_id,
            "message": f"Market discovery started with {len(seeds)} seeds",
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/discovery/market/{flow_id}/status")
async def get_discovery_status(flow_id: str):
    """Get current status and checkpoint data for a discovery flow."""
    try:
        checkpoint_mgr = get_checkpoint_manager()
        
        # Load flow state
        state = checkpoint_mgr.load_flow_state(flow_id)
        if not state:
            return {"status": "error", "message": "Flow not found"}
        
        # Get latest checkpoint
        latest = checkpoint_mgr.get_latest_checkpoint(flow_id)
        
        # List all checkpoints
        all_checkpoints = checkpoint_mgr.list_checkpoints(flow_id)
        
        return {
            "status": "success",
            "flow_id": flow_id,
            "current_phase": state.current_phase.value,
            "created_at": state.created_at.isoformat(),
            "latest_checkpoint": latest.phase.value if latest else None,
            "checkpoints": [
                {
                    "phase": c.phase.value,
                    "phase_number": c.phase_number,
                    "timestamp": c.timestamp.isoformat(),
                    "item_count": c.item_count,
                }
                for c in all_checkpoints
            ],
            "progress": {
                "seeds_analyzed": len(state.seed_profiles),
                "candidates_discovered": len(state.discovered_candidates),
                "candidates_classified": len(state.classifications),
                "sources_found": len(state.third_party_sources),
            }
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/discovery/market/{flow_id}/checkpoint/{phase}")
async def get_checkpoint_data(flow_id: str, phase: str):
    """Get detailed data at a specific checkpoint for review."""
    try:
        from prefect_workflows.models import DiscoveryPhase
        
        checkpoint_mgr = get_checkpoint_manager()
        checkpoint_phase = DiscoveryPhase(phase)
        
        checkpoint = checkpoint_mgr.load_checkpoint(flow_id, checkpoint_phase)
        if not checkpoint:
            return {"status": "error", "message": f"Checkpoint {phase} not found"}
        
        # Convert to dict for JSON response
        return {
            "status": "success",
            "checkpoint": checkpoint.model_dump(mode="json"),
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.post("/api/discovery/market/{flow_id}/resume")
async def resume_from_checkpoint(flow_id: str, request: CheckpointReviewRequest):
    """
    Resume a paused flow with user corrections.
    
    This allows the human-in-the-loop to:
    - Remove bad candidates
    - Reclassify candidates
    - Edit descriptions
    - Approve and continue
    """
    try:
        checkpoint_mgr = get_checkpoint_manager()
        
        # Save user review
        from prefect_workflows.models import DiscoveryPhase, UserReviewResult
        
        review = UserReviewResult(
            checkpoint_id=f"{flow_id}:{request.phase}",
            approved=request.approved,
            modified=bool(request.modifications),
            removed_candidate_ids=request.modifications.get("removed_candidate_ids", []) if request.modifications else [],
            modified_classifications=request.modifications.get("modified_classifications", []) if request.modifications else [],
        )
        
        phase_enum = DiscoveryPhase(request.phase)
        checkpoint_mgr.save_user_review(flow_id, phase_enum, review)
        
        # Resume flow (would need Prefect integration here)
        # For now, mark for resumption
        
        return {
            "status": "resumed",
            "flow_id": flow_id,
            "phase": request.phase,
            "message": "Flow resuming with user corrections",
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/discovery/market/{flow_id}/result")
async def get_discovery_result(flow_id: str):
    """Get final result of a completed discovery flow."""
    try:
        checkpoint_mgr = get_checkpoint_manager()
        state = checkpoint_mgr.load_flow_state(flow_id)
        
        if not state:
            return {"status": "error", "message": "Flow not found"}
        
        if state.current_phase.value != "complete":
            return {
                "status": "in_progress",
                "current_phase": state.current_phase.value,
                "message": "Flow not yet complete",
            }
        
        # Return completed results
        return {
            "status": "complete",
            "flow_id": flow_id,
            "market_map": {
                "direct_competitors": [
                    {"name": c.name, "website": c.website, "description": c.description}
                    for c in state.approved_candidates  # Would filter by classification
                ],
            },
            "lead_sites": [
                {"name": s.site_name, "usefulness": s.usefulness_score}
                for s in state.lead_sites
            ],
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}
