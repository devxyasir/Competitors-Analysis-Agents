"""
Checkpoint Manager - State Persistence for Human-in-the-Loop

Manages saving/loading flow state at checkpoints for pause/resume functionality.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from prefect_workflows.models import (
    CheckpointData,
    FlowState,
    UserReviewResult,
    DiscoveryPhase,
)


# Use same database as main app
CHECKPOINT_DIR = Path("history/checkpoints")
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


class CheckpointManager:
    """Manages checkpoints for pause/resume functionality."""
    
    def __init__(self, checkpoint_dir: Path = CHECKPOINT_DIR):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_flow_path(self, flow_id: str) -> Path:
        """Get directory for a specific flow's checkpoints."""
        flow_path = self.checkpoint_dir / flow_id
        flow_path.mkdir(exist_ok=True)
        return flow_path
    
    def _get_checkpoint_path(self, flow_id: str, phase: DiscoveryPhase) -> Path:
        """Get file path for a specific checkpoint."""
        flow_path = self._get_flow_path(flow_id)
        return flow_path / f"{phase.value}.json"
    
    def _get_state_path(self, flow_id: str) -> Path:
        """Get file path for full flow state."""
        flow_path = self._get_flow_path(flow_id)
        return flow_path / "state.json"
    
    def save_checkpoint(
        self,
        flow_id: str,
        checkpoint_data: CheckpointData,
        flow_state: Optional[FlowState] = None
    ) -> str:
        """
        Save a checkpoint for later review/resume.
        
        Returns checkpoint_id (which is flow_id:phase)
        """
        checkpoint_id = f"{flow_id}:{checkpoint_data.phase.value}"
        
        # Save checkpoint data
        checkpoint_path = self._get_checkpoint_path(flow_id, checkpoint_data.phase)
        checkpoint_path.write_text(
            checkpoint_data.model_dump_json(indent=2),
            encoding="utf-8"
        )
        
        # Also save full flow state if provided
        if flow_state:
            self.save_flow_state(flow_state)
        
        return checkpoint_id
    
    def load_checkpoint(self, flow_id: str, phase: DiscoveryPhase) -> Optional[CheckpointData]:
        """Load a specific checkpoint."""
        checkpoint_path = self._get_checkpoint_path(flow_id, phase)
        
        if not checkpoint_path.exists():
            return None
        
        data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return CheckpointData.model_validate(data)
    
    def save_flow_state(self, state: FlowState) -> None:
        """Save the complete flow state."""
        state_path = self._get_state_path(state.flow_id)
        state_path.write_text(
            state.model_dump_json(indent=2),
            encoding="utf-8"
        )
    
    def load_flow_state(self, flow_id: str) -> Optional[FlowState]:
        """Load complete flow state."""
        state_path = self._get_state_path(flow_id)
        
        if not state_path.exists():
            return None
        
        data = json.loads(state_path.read_text(encoding="utf-8"))
        return FlowState.model_validate(data)
    
    def save_user_review(
        self,
        flow_id: str,
        phase: DiscoveryPhase,
        review: UserReviewResult
    ) -> None:
        """Save user's review/corrections at a checkpoint."""
        flow_path = self._get_flow_path(flow_id)
        review_path = flow_path / f"{phase.value}_review.json"
        
        review_path.write_text(
            review.model_dump_json(indent=2),
            encoding="utf-8"
        )
    
    def load_user_review(
        self,
        flow_id: str,
        phase: DiscoveryPhase
    ) -> Optional[UserReviewResult]:
        """Load user's review for a checkpoint."""
        flow_path = self._get_flow_path(flow_id)
        review_path = flow_path / f"{phase.value}_review.json"
        
        if not review_path.exists():
            return None
        
        data = json.loads(review_path.read_text(encoding="utf-8"))
        return UserReviewResult.model_validate(data)
    
    def get_latest_checkpoint(self, flow_id: str) -> Optional[CheckpointData]:
        """Get the most recent checkpoint for a flow."""
        flow_path = self._get_flow_path(flow_id)
        
        if not flow_path.exists():
            return None
        
        # Find all checkpoint files
        checkpoints = []
        for file_path in flow_path.glob("*.json"):
            if file_path.name in ["state.json"] or "_review" in file_path.name:
                continue
            
            phase_name = file_path.stem
            try:
                phase = DiscoveryPhase(phase_name)
                checkpoint = self.load_checkpoint(flow_id, phase)
                if checkpoint:
                    checkpoints.append(checkpoint)
            except ValueError:
                continue
        
        if not checkpoints:
            return None
        
        # Return most recent by timestamp
        return max(checkpoints, key=lambda c: c.timestamp)
    
    def list_checkpoints(self, flow_id: str) -> list[CheckpointData]:
        """List all checkpoints for a flow, ordered by timestamp."""
        flow_path = self._get_flow_path(flow_id)
        
        if not flow_path.exists():
            return []
        
        checkpoints = []
        for phase in DiscoveryPhase:
            checkpoint = self.load_checkpoint(flow_id, phase)
            if checkpoint:
                checkpoints.append(checkpoint)
        
        return sorted(checkpoints, key=lambda c: c.timestamp)
    
    def delete_flow_checkpoints(self, flow_id: str) -> None:
        """Clean up all checkpoints for a flow."""
        flow_path = self._get_flow_path(flow_id)
        
        if flow_path.exists():
            import shutil
            shutil.rmtree(flow_path)


# Global instance
_checkpoint_manager: Optional[CheckpointManager] = None


def get_checkpoint_manager() -> CheckpointManager:
    """Get or create the global checkpoint manager."""
    global _checkpoint_manager
    if _checkpoint_manager is None:
        _checkpoint_manager = CheckpointManager()
    return _checkpoint_manager
