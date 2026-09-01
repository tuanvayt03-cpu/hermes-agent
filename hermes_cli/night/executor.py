"""Platform abstraction with only a non-destructive executor in V1."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HibernateReceipt:
    accepted: bool
    dry_run: bool
    platform: str
    action: str


class DryRunHibernateExecutor:
    """Records the intended action and never calls an operating-system API."""

    dry_run = True

    def __init__(self, platform: str = "mock") -> None:
        self.platform = platform
        self.execution_count = 0

    def execute(self, night_session_id: str) -> HibernateReceipt:
        self.execution_count += 1
        return HibernateReceipt(
            accepted=True,
            dry_run=True,
            platform=self.platform,
            action=f"DRY_RUN_HIBERNATE:{night_session_id}",
        )
