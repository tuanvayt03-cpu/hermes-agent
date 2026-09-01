"""Dedicated durable storage for Night manifests and state receipts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping
from uuid import uuid4

from .contracts import MasterTaskSnapshot


_SECRET_PATTERN = re.compile(
    r"(?i)(api[_-]?key|client[_-]?secret|password|authorization|bearer\s+|token\s*[:=])"
)


class ManifestError(RuntimeError):
    pass


class ManifestMissingError(ManifestError):
    pass


class ManifestCorruptError(ManifestError):
    pass


class NightStateConflict(ManifestError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_night_session_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"night-{stamp}-{uuid4().hex[:12]}"


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _reject_secrets(value: Any, location: str = "manifest") -> None:
    if isinstance(value, str) and _SECRET_PATTERN.search(value):
        raise ManifestError(f"secret-like material rejected at {location}")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _reject_secrets(item, f"{location}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_secrets(item, f"{location}[{index}]")


@dataclass(frozen=True)
class ResumeManifest:
    MASTER_TASK_ID: str
    MASTER_TASK_NAME: str
    TASK_STATUS: str
    CURRENT_CAPABILITY: str
    FIRST_UNPROVEN_BOUNDARY: str
    LAST_DURABLE_CHECKPOINT: str
    ACTIVE_WORKERS: tuple[str, ...]
    PROCESS_IDENTITIES_IF_REQUIRED: tuple[dict[str, Any], ...]
    PENDING_OPERATOR_ACTION: str
    RESUME_INSTRUCTION: str
    CREATED_AT: str
    MANIFEST_ID: str
    MANIFEST_HASH: str = ""
    REPOSITORY_HEAD: str = ""
    RUNTIME_IDENTITY: str = ""

    @classmethod
    def from_task(
        cls,
        task: MasterTaskSnapshot,
        *,
        night_session_id: str,
        created_at: str | None = None,
    ) -> "ResumeManifest":
        manifest = cls(
            MASTER_TASK_ID=task.master_task_id,
            MASTER_TASK_NAME=task.master_task_name,
            TASK_STATUS=task.task_status,
            CURRENT_CAPABILITY=task.current_capability,
            FIRST_UNPROVEN_BOUNDARY=task.first_unproven_boundary,
            LAST_DURABLE_CHECKPOINT=task.last_durable_checkpoint,
            ACTIVE_WORKERS=tuple(task.active_workers),
            PROCESS_IDENTITIES_IF_REQUIRED=tuple(
                asdict(identity) for identity in task.process_identities_if_required
            ),
            PENDING_OPERATOR_ACTION=task.pending_operator_action,
            RESUME_INSTRUCTION=task.resume_instruction,
            CREATED_AT=created_at or utc_now(),
            MANIFEST_ID=f"manifest-{night_session_id}",
            REPOSITORY_HEAD=task.repository_head,
            RUNTIME_IDENTITY=task.runtime_identity,
        )
        return replace(manifest, MANIFEST_HASH=manifest.calculate_hash())

    def unsigned_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("MANIFEST_HASH", None)
        return payload

    def calculate_hash(self) -> str:
        return hashlib.sha256(_canonical_json(self.unsigned_payload())).hexdigest()

    def validate(self) -> None:
        required = {
            "MASTER_TASK_ID": self.MASTER_TASK_ID,
            "MASTER_TASK_NAME": self.MASTER_TASK_NAME,
            "TASK_STATUS": self.TASK_STATUS,
            "CURRENT_CAPABILITY": self.CURRENT_CAPABILITY,
            "FIRST_UNPROVEN_BOUNDARY": self.FIRST_UNPROVEN_BOUNDARY,
            "LAST_DURABLE_CHECKPOINT": self.LAST_DURABLE_CHECKPOINT,
            "RESUME_INSTRUCTION": self.RESUME_INSTRUCTION,
            "CREATED_AT": self.CREATED_AT,
            "MANIFEST_ID": self.MANIFEST_ID,
            "MANIFEST_HASH": self.MANIFEST_HASH,
        }
        missing = [key for key, value in required.items() if not str(value).strip()]
        if missing:
            raise ManifestCorruptError(f"manifest fields missing: {', '.join(missing)}")
        _reject_secrets(asdict(self))
        if self.MANIFEST_HASH != self.calculate_hash():
            raise ManifestCorruptError("manifest SHA-256 mismatch")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ResumeManifest":
        try:
            normalized = dict(payload)
            normalized["ACTIVE_WORKERS"] = tuple(normalized.get("ACTIVE_WORKERS", ()))
            normalized["PROCESS_IDENTITIES_IF_REQUIRED"] = tuple(
                normalized.get("PROCESS_IDENTITIES_IF_REQUIRED", ())
            )
            manifest = cls(**normalized)
        except (KeyError, TypeError, ValueError) as exc:
            raise ManifestCorruptError(f"invalid manifest schema: {exc}") from exc
        manifest.validate()
        return manifest


class DurableNightStore:
    """JSON store isolated from state.db, kanban.db, and watchdog databases."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).expanduser().resolve()
        self.manifest_dir = self.root / "manifests"
        self.session_dir = self.root / "sessions"
        self.lock_dir = self.root / "locks"
        for directory in (self.manifest_dir, self.session_dir, self.lock_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self._thread_lock = threading.RLock()

    def _atomic_write(self, path: Path, payload: Mapping[str, Any]) -> None:
        _reject_secrets(payload, path.name)
        encoded = _canonical_json(payload) + b"\n"
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
            ) as handle:
                temp_path = Path(handle.name)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            temp_path = None
            self._fsync_directory(path.parent)
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink()
                except FileNotFoundError:
                    pass

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        if os.name == "nt":
            return
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @contextmanager
    def session_lock(self, night_session_id: str) -> Iterator[None]:
        lock_path = self.lock_dir / f"{night_session_id}.lock"
        with self._thread_lock:
            try:
                descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError as exc:
                raise NightStateConflict(
                    f"night session {night_session_id} has an unresolved owner lock"
                ) from exc
            try:
                os.write(descriptor, str(os.getpid()).encode("ascii"))
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            try:
                yield
            finally:
                try:
                    lock_path.unlink()
                except FileNotFoundError:
                    pass

    def write_manifest(self, manifest: ResumeManifest) -> Path:
        path = self.manifest_dir / f"{manifest.MANIFEST_ID}.json"
        self._atomic_write(path, manifest.to_dict())
        read_back = self.read_manifest(manifest.MANIFEST_ID)
        if read_back.MANIFEST_HASH != manifest.MANIFEST_HASH:
            raise ManifestCorruptError("manifest read-back differs from durable write")
        return path

    def read_manifest(self, manifest_id: str) -> ResumeManifest:
        path = self.manifest_dir / f"{manifest_id}.json"
        if not path.is_file():
            raise ManifestMissingError(f"resume manifest missing: {manifest_id}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ManifestCorruptError(f"resume manifest unreadable: {manifest_id}") from exc
        if not isinstance(payload, dict):
            raise ManifestCorruptError("resume manifest root must be an object")
        return ResumeManifest.from_dict(payload)

    def create_session(self, night_session_id: str, manifest: ResumeManifest) -> None:
        state = {
            "night_session_id": night_session_id,
            "manifest_id": manifest.MANIFEST_ID,
            "manifest_hash": manifest.MANIFEST_HASH,
            "master_task_id": manifest.MASTER_TASK_ID,
            "phase": "MANIFEST_DURABLE",
            "notification_phase": "NOT_STARTED",
            "notification_dedup_key": f"night:{night_session_id}:NIGHT_HIBERNATION_READY",
            "hibernate_dry_run": False,
            "resume_count": 0,
            "watchdog_recovery_action": "NOOP",
            "updated_at": utc_now(),
        }
        path = self.session_dir / f"{night_session_id}.json"
        with self.session_lock(night_session_id):
            if path.exists():
                raise NightStateConflict(f"night session already exists: {night_session_id}")
            self._atomic_write(path, state)

    def read_session(self, night_session_id: str) -> dict[str, Any]:
        path = self.session_dir / f"{night_session_id}.json"
        if not path.is_file():
            raise ManifestMissingError(f"night session missing: {night_session_id}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ManifestCorruptError(f"night session unreadable: {night_session_id}") from exc
        if not isinstance(payload, dict):
            raise ManifestCorruptError("night session root must be an object")
        return payload

    def transition(
        self,
        night_session_id: str,
        *,
        expected_phase: str | tuple[str, ...] | None = None,
        **updates: Any,
    ) -> dict[str, Any]:
        allowed = (expected_phase,) if isinstance(expected_phase, str) else expected_phase
        with self.session_lock(night_session_id):
            state = self.read_session(night_session_id)
            if allowed is not None and state.get("phase") not in allowed:
                raise NightStateConflict(
                    f"phase {state.get('phase')} not in expected {tuple(allowed)}"
                )
            state.update(updates)
            state["updated_at"] = utc_now()
            self._atomic_write(self.session_dir / f"{night_session_id}.json", state)
            return state

    def unresolved_session_ids(self) -> tuple[str, ...]:
        unresolved: list[str] = []
        for path in sorted(self.session_dir.glob("night-*.json")):
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                unresolved.append(path.stem)
                continue
            if state.get("phase") not in {"RESUMED", "ABORTED"}:
                unresolved.append(str(state.get("night_session_id") or path.stem))
        return tuple(unresolved)
