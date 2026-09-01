"""Canonical, fail-closed /night lifecycle candidate.

The package is intentionally not wired into the live slash-command registry
until the Notification V3 dependency and the live join gate are accepted.
"""

from .command import NightCommand, NightCommandRequest
from .contracts import (
    AdmissionEvidence,
    MasterTaskSnapshot,
    NightOutcome,
    ProcessIdentity,
    QuiescenceEvidence,
    RecoveryEvidence,
)
from .executor import DryRunHibernateExecutor
from .manifest import DurableNightStore, ResumeManifest
from .notification import NotificationV3Adapter
from .workflow import NightWorkflow

__all__ = [
    "AdmissionEvidence",
    "DryRunHibernateExecutor",
    "DurableNightStore",
    "MasterTaskSnapshot",
    "NightCommand",
    "NightCommandRequest",
    "NightOutcome",
    "NightWorkflow",
    "NotificationV3Adapter",
    "ProcessIdentity",
    "QuiescenceEvidence",
    "RecoveryEvidence",
    "ResumeManifest",
]
