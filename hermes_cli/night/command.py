"""Candidate /night command boundary.

Live registry wiring is intentionally deferred until Notification V3 Phase 10
and the external live join gate pass.  This module is the single command owner
once activated; it does not infer requests from inactivity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .contracts import NightOutcome, SnapshotProvider
    from .workflow import NightWorkflow


@dataclass(frozen=True)
class NightCommandRequest:
    raw_command: str
    NIGHT_REQUESTED_BY_OPERATOR: bool


class NightCommand:
    def __init__(self, workflow: "NightWorkflow") -> None:
        self._workflow = workflow

    def execute(
        self, request: NightCommandRequest, snapshot_provider: "SnapshotProvider"
    ) -> "NightOutcome":
        if request.raw_command.strip().lower() != "/night":
            raise ValueError("night command requires the exact /night operator command")
        return self._workflow.request_night(
            snapshot_provider,
            requested_by_operator=request.NIGHT_REQUESTED_BY_OPERATOR,
        )
