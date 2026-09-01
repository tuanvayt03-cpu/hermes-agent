"""Notification V3 semantic event construction; no transport lives here."""

from __future__ import annotations

from dataclasses import dataclass

from .manifest import ResumeManifest


@dataclass(frozen=True)
class NightHibernationReadyEvent:
    event_type: str
    task_name: str
    task_id: str
    status: str
    summary: str
    useful_verification: str
    next_action: str
    dedup_key: str
    consent_permission: str = ""
    consent_reason: str = ""
    consent_request_id: str = ""
    source_kind: str = "night"
    board: str = ""
    is_terminal: bool = False
    authoritative: bool = True


class NotificationV3Adapter:
    """Thin adapter over V3's exact ``route(event, telegram_sender)`` API.

    The sender is injected by the existing gateway delivery owner.  This
    adapter owns neither formatting nor transport and therefore cannot become
    a competing notification authority.
    """

    def __init__(self, canonical_router, canonical_sender) -> None:
        self._canonical_router = canonical_router
        self._canonical_sender = canonical_sender

    def route(self, event: NightHibernationReadyEvent):
        return self._canonical_router.route(event, self._canonical_sender)


def build_night_hibernation_ready_event(
    night_session_id: str, manifest: ResumeManifest
) -> NightHibernationReadyEvent:
    return NightHibernationReadyEvent(
        event_type="NIGHT_HIBERNATION_READY",
        task_name=manifest.MASTER_TASK_NAME,
        task_id=manifest.MASTER_TASK_ID,
        status=manifest.TASK_STATUS,
        summary=(
            f"Task đang ở trạng thái {manifest.TASK_STATUS}; checkpoint đã lưu: "
            f"{manifest.LAST_DURABLE_CHECKPOINT}. Machine chuẩn bị sleep."
        ),
        useful_verification=(
            f"Resume manifest {manifest.MANIFEST_ID} đã được xác minh bằng SHA-256."
        ),
        next_action=(
            "Sáng dậy Hermes sẽ resume đúng task này từ boundary: "
            f"{manifest.FIRST_UNPROVEN_BOUNDARY}."
        ),
        dedup_key=f"night:{night_session_id}:NIGHT_HIBERNATION_READY",
    )
