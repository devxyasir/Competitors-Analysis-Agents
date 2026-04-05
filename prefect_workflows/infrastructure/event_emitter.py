"""
Event Emitter - Real-time Frontend Notifications

Emits events during flow execution for live frontend updates.
"""

from datetime import datetime
from typing import Any, Optional
from prefect import get_run_logger
from prefect.events import emit_event as prefect_emit_event

from prefect_workflows.models import FlowEvent, DiscoveryPhase, TaskProgress


class FlowEventEmitter:
    """Emits events to update the frontend in real-time."""
    
    def __init__(self, flow_id: str):
        self.flow_id = flow_id
        self.logger = get_run_logger()
    
    def _emit(
        self,
        event_type: str,
        phase: Optional[DiscoveryPhase] = None,
        payload: dict[str, Any] = None
    ) -> None:
        """Emit an event to Prefect's event system."""
        payload = payload or {}
        
        # Log locally
        self.logger.info(f"[{self.flow_id}] {event_type}: {payload.get('message', '')}")
        
        # Emit to Prefect (for UI and external listeners)
        try:
            prefect_emit_event(
                event=event_type,
                resource={
                    "prefect.resource.id": f"discovery-flow/{self.flow_id}",
                    "prefect.resource.name": f"Market Discovery {self.flow_id[:8]}",
                    "flow_id": self.flow_id,
                    "phase": phase.value if phase else None,
                },
                payload={
                    "flow_id": self.flow_id,
                    "phase": phase.value if phase else None,
                    **payload
                }
            )
        except Exception as e:
            # Don't fail the flow if event emission fails
            self.logger.warning(f"Failed to emit event: {e}")
    
    # =========================================================================
    # Phase Events
    # =========================================================================
    
    def phase_started(self, phase: DiscoveryPhase, message: str = "") -> None:
        """Announce start of a new phase."""
        self._emit(
            "discovery.phase.started",
            phase=phase,
            payload={
                "message": message or f"Starting {phase.value}",
                "phase_number": self._get_phase_number(phase),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    def phase_completed(self, phase: DiscoveryPhase, items_count: int = 0) -> None:
        """Announce completion of a phase."""
        self._emit(
            "discovery.phase.completed",
            phase=phase,
            payload={
                "items_count": items_count,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    def checkpoint_reached(
        self,
        phase: DiscoveryPhase,
        items_for_review: int,
        checkpoint_id: str
    ) -> None:
        """Announce checkpoint reached (flow will pause)."""
        self._emit(
            "discovery.checkpoint.reached",
            phase=phase,
            payload={
                "checkpoint_id": checkpoint_id,
                "items_for_review": items_for_review,
                "phase_number": self._get_phase_number(phase),
                "requires_action": True,
                "message": f"Checkpoint {self._get_phase_number(phase)}: {items_for_review} items ready for review"
            }
        )
    
    def checkpoint_resumed(self, phase: DiscoveryPhase, checkpoint_id: str) -> None:
        """Announce checkpoint resumed."""
        self._emit(
            "discovery.checkpoint.resumed",
            phase=phase,
            payload={
                "checkpoint_id": checkpoint_id,
                "message": f"Resuming from checkpoint"
            }
        )
    
    # =========================================================================
    # Task Progress Events
    # =========================================================================
    
    def task_progress(self, progress: TaskProgress) -> None:
        """Emit progress update for a running task."""
        self._emit(
            "discovery.task.progress",
            payload={
                "task_name": progress.task_name,
                "current": progress.current,
                "total": progress.total,
                "percentage": progress.percentage,
                "message": progress.message
            }
        )
    
    def task_completed(self, task_name: str, result_summary: str = "") -> None:
        """Announce task completion."""
        self._emit(
            "discovery.task.completed",
            payload={
                "task_name": task_name,
                "result_summary": result_summary
            }
        )
    
    def task_failed(self, task_name: str, error: str, will_retry: bool = False) -> None:
        """Announce task failure."""
        self._emit(
            "discovery.task.failed",
            payload={
                "task_name": task_name,
                "error": error,
                "will_retry": will_retry
            }
        )
    
    # =========================================================================
    # Data Discovery Events
    # =========================================================================
    
    def seed_analyzed(self, seed_name: str, profile_summary: str) -> None:
        """Announce a seed has been analyzed."""
        self._emit(
            "discovery.seed.analyzed",
            phase=DiscoveryPhase.SEED_ANALYSIS,
            payload={
                "seed_name": seed_name,
                "profile_summary": profile_summary
            }
        )
    
    def candidates_discovered(self, count: int, from_sources: list[str]) -> None:
        """Announce candidates have been discovered."""
        self._emit(
            "discovery.candidates.discovered",
            phase=DiscoveryPhase.CANDIDATE_DISCOVERY,
            payload={
                "count": count,
                "from_sources": from_sources
            }
        )
    
    def candidate_classified(
        self,
        candidate_name: str,
        category: str,
        confidence: float
    ) -> None:
        """Announce a candidate has been classified."""
        self._emit(
            "discovery.candidate.classified",
            phase=DiscoveryPhase.CLASSIFICATION,
            payload={
                "candidate_name": candidate_name,
                "category": category,
                "confidence": confidence
            }
        )
    
    def sources_discovered(self, candidate_name: str, count: int) -> None:
        """Announce sources found for a candidate."""
        self._emit(
            "discovery.sources.discovered",
            phase=DiscoveryPhase.SOURCE_DISCOVERY,
            payload={
                "candidate_name": candidate_name,
                "count": count
            }
        )
    
    # =========================================================================
    # User Action Events
    # =========================================================================
    
    def user_correction_applied(
        self,
        checkpoint_phase: DiscoveryPhase,
        correction_type: str,
        details: dict[str, Any]
    ) -> None:
        """Announce user corrections have been applied."""
        self._emit(
            "discovery.user.correction_applied",
            phase=checkpoint_phase,
            payload={
                "correction_type": correction_type,
                "details": details
            }
        )
    
    def flow_completed(self, result_summary: dict[str, Any]) -> None:
        """Announce flow completion."""
        self._emit(
            "discovery.flow.completed",
            payload={
                "result_summary": result_summary,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    def flow_failed(self, error: str, failed_phase: Optional[DiscoveryPhase] = None) -> None:
        """Announce flow failure."""
        self._emit(
            "discovery.flow.failed",
            phase=failed_phase,
            payload={
                "error": error,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    # =========================================================================
    # Helpers
    # =========================================================================
    
    def _get_phase_number(self, phase: DiscoveryPhase) -> int:
        """Get the phase number for display."""
        mapping = {
            DiscoveryPhase.SEED_ANALYSIS: 1,
            DiscoveryPhase.SEARCH_DIRECTIONS: 2,
            DiscoveryPhase.CANDIDATE_DISCOVERY: 3,
            DiscoveryPhase.CLASSIFICATION: 4,
            DiscoveryPhase.SOURCE_DISCOVERY: 5,
            DiscoveryPhase.COMPLETE: 6,
        }
        return mapping.get(phase, 0)


# Factory function
def create_emitter(flow_id: str) -> FlowEventEmitter:
    """Create an event emitter for a flow."""
    return FlowEventEmitter(flow_id)
