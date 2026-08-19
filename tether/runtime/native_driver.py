"""Detached-native exact-turn driver.

Owns the process lifecycle for `detached_native` attempts and is the only
producer of their driver receipts. The safety order is fixed:

    durable spawn intent  ->  spawn (setsid, detached)
      ->  process identity captured (pid + starttime)
      ->  durable `accepted` receipt
      ->  watch/reap  ->  exact terminal receipt

A crash anywhere after the durable intent and before the `accepted` receipt
loses the spawn proof: recovery classifies the attempt `uncertain` and never
re-executes. After `accepted`, the captured process identity makes recovery a
pure observation (alive -> keep watching; exited -> read the response file).

Response contract at clean exit (status 0): an absent or empty response file
is `failed`; exactly `NO_REPLY` after strip is `no_reply`; anything else is
`completed_with_response` with the payload stored as an owner-private
content-addressed blob. A nonzero exit is `failed`. Cancellation kills the
process group; an unobservable kill outcome is `uncertain`, never `cancelled`.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import signal
import subprocess  # nosec B404
import time
from pathlib import Path
from typing import Any

import domain_runtime as domain_runtime_module
import security


class NativeDriverError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


def _fsync_file_and_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _process_starttime(pid: int) -> str | None:
    try:
        stat_line = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (FileNotFoundError, ProcessLookupError, OSError):
        return None
    # Field 22 (starttime) counted after the parenthesized comm field.
    return stat_line.rsplit(")", 1)[1].split()[19]


class NativeDriver:
    def __init__(
        self,
        runtime: domain_runtime_module.DomainRuntime,
        *,
        work_root: Path,
    ):
        self.runtime = runtime
        self.work_root = Path(work_root)
        security.secure_state_directory(self.work_root, create=True)
        self.blob_root = self.work_root / "blobs"
        security.secure_state_directory(self.blob_root, create=True)

    # -- private per-attempt state -------------------------------------------

    def _attempt_dir(self, attempt_id: str) -> Path:
        directory = self.work_root / attempt_id
        security.secure_state_directory(directory, create=True)
        return directory

    def _journal_path(self, attempt_id: str) -> Path:
        return self._attempt_dir(attempt_id) / "journal.json"

    def _read_journal(self, attempt_id: str) -> dict[str, Any] | None:
        path = self._journal_path(attempt_id)
        try:
            raw, _identity = security.read_owned_file_bytes(
                path,
                max_bytes=64 * 1024,
                expected_mode=0o600,
            )
        except FileNotFoundError:
            return None
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise NativeDriverError("journal_corrupt") from exc
        if not isinstance(value, dict):
            raise NativeDriverError("journal_corrupt")
        return value

    def _write_journal(self, attempt_id: str, value: dict[str, Any]) -> None:
        path = self._journal_path(attempt_id)
        staging = path.with_name(path.name + f".tmp-{os.getpid()}")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(staging, flags, 0o600)
        try:
            os.write(descriptor, json.dumps(value, sort_keys=True).encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(staging, path)
        _fsync_file_and_dir(path)

    def _store_blob(self, content: bytes) -> tuple[str, str, int]:
        digest = hashlib.sha256(content).hexdigest()
        target = self.blob_root / digest
        if not target.exists():
            staging = self.blob_root / f".tmp-{os.getpid()}-{digest[:16]}"
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(staging, flags, 0o600)
            try:
                os.write(descriptor, content)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(staging, target)
            _fsync_file_and_dir(target)
        return f"blob:sha256:{digest}", digest, len(content)

    def _next_sequence(self, attempt_id: str) -> int:
        return int(self.runtime.attempt_status(attempt_id)["last_driver_sequence"]) + 1

    def _emit(
        self,
        attempt: dict[str, Any],
        *,
        state: str,
        operation: str = "submit",
        request_id: str | None = None,
        error_code: str | None = None,
        response: tuple[str, str, int] | None = None,
        incarnation: str,
    ) -> dict[str, Any]:
        sequence = self._next_sequence(attempt["attempt_id"])
        arguments: dict[str, Any] = dict(
            attempt_id=attempt["attempt_id"],
            receipt_id=f"drv-{attempt['attempt_id']}-{sequence}",
            lease_fence=attempt["lease_fence"],
            sequence=sequence,
            driver_incarnation=incarnation,
            operation=operation,
            request_id=request_id or attempt["driver_request_id"],
            watch_cursor=f"proc:{incarnation}:{sequence}",
            state=state,
            observed_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            error_code=error_code,
        )
        if response is not None:
            arguments.update(
                response_ref=response[0],
                response_sha256=response[1],
                response_bytes=response[2],
            )
        return self.runtime.record_driver_receipt(**arguments)

    # -- launch ---------------------------------------------------------------

    def launch(
        self,
        attempt: dict[str, Any],
        *,
        command: list[str],
        cwd: Path,
        env: dict[str, str],
        fault_inject: Any | None = None,
    ) -> dict[str, Any]:
        """Spawn the harness process and durably record acceptance."""
        attempt_id = attempt["attempt_id"]
        if self._read_journal(attempt_id) is not None:
            raise NativeDriverError(
                "attempt_already_launched",
                "a spawn intent already exists; recovery must classify it first",
            )
        self.runtime.mark_submitting(attempt_id)
        work = self._attempt_dir(attempt_id)
        response_path = work / "response.out"
        stderr_path = work / "stderr.log"
        # Durable intent BEFORE spawn: from here on, a lost proof is uncertain.
        self._write_journal(attempt_id, {
            "phase": "spawning",
            "request_id": attempt["driver_request_id"],
            "lease_fence": attempt["lease_fence"],
        })
        if fault_inject is not None:
            fault_inject("after_intent")
        with open(response_path, "wb") as stdout, open(stderr_path, "wb") as stderr:
            os.chmod(response_path, 0o600)
            os.chmod(stderr_path, 0o600)
            process = subprocess.Popen(  # nosec B603
                command,
                cwd=str(cwd),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
        starttime = _process_starttime(process.pid)
        incarnation = f"{process.pid}:{starttime or 'exited'}"
        if fault_inject is not None:
            fault_inject("after_spawn")
        self._write_journal(attempt_id, {
            "phase": "spawned",
            "request_id": attempt["driver_request_id"],
            "lease_fence": attempt["lease_fence"],
            "pid": process.pid,
            "starttime": starttime,
        })
        self._emit(attempt, state="accepted", incarnation=incarnation)
        self._write_journal(attempt_id, {
            "phase": "accepted",
            "request_id": attempt["driver_request_id"],
            "lease_fence": attempt["lease_fence"],
            "pid": process.pid,
            "starttime": starttime,
        })
        return {
            "attempt_id": attempt_id,
            "pid": process.pid,
            "incarnation": incarnation,
            "process": process,
            "response_path": response_path,
        }

    # -- completion -----------------------------------------------------------

    def reap(
        self,
        attempt: dict[str, Any],
        launched: dict[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Wait for exit and convert the observed outcome into one receipt."""
        process: subprocess.Popen[bytes] = launched["process"]
        try:
            exit_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            raise NativeDriverError("attempt_still_running") from exc
        return self._terminal_receipt_from_exit(
            attempt,
            incarnation=launched["incarnation"],
            response_path=launched["response_path"],
            exit_code=exit_code,
        )

    def _terminal_receipt_from_exit(
        self,
        attempt: dict[str, Any],
        *,
        incarnation: str,
        response_path: Path,
        exit_code: int,
    ) -> dict[str, Any]:
        attempt_id = attempt["attempt_id"]
        if exit_code != 0:
            result = self._emit(
                attempt,
                state="failed",
                error_code=f"exit_{exit_code}",
                incarnation=incarnation,
            )
        else:
            try:
                content = response_path.read_bytes()
            except FileNotFoundError:
                content = b""
            text = content.decode("utf-8", errors="replace")
            if not text.strip():
                result = self._emit(
                    attempt,
                    state="failed",
                    error_code="empty_response",
                    incarnation=incarnation,
                )
            elif domain_runtime_module.is_no_reply(text):
                result = self._emit(
                    attempt,
                    state="no_reply",
                    incarnation=incarnation,
                )
            else:
                result = self._emit(
                    attempt,
                    state="completed_with_response",
                    response=self._store_blob(content),
                    incarnation=incarnation,
                )
        self._write_journal(attempt_id, {
            "phase": "terminal",
            "request_id": attempt["driver_request_id"],
            "lease_fence": attempt["lease_fence"],
            "state": result["state"],
        })
        return result

    # -- cancellation -----------------------------------------------------------

    def cancel(
        self,
        attempt: dict[str, Any],
        launched: dict[str, Any],
        *,
        cancel_request_id: str,
        grace_seconds: float = 5.0,
    ) -> dict[str, Any]:
        self.runtime.request_cancel(attempt["attempt_id"], cancel_request_id)
        process: subprocess.Popen[bytes] = launched["process"]
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)
            try:
                process.wait(timeout=grace_seconds)
            except subprocess.TimeoutExpired:
                # The kill outcome is unobservable: possible execution
                # continues somewhere we cannot see. Never claim cancelled.
                return self._emit(
                    attempt,
                    state="uncertain",
                    operation="cancel",
                    request_id=cancel_request_id,
                    error_code="cancel_unobservable",
                    incarnation=launched["incarnation"],
                )
        result = self._emit(
            attempt,
            state="cancelled",
            operation="cancel",
            request_id=cancel_request_id,
            incarnation=launched["incarnation"],
        )
        self._write_journal(attempt["attempt_id"], {
            "phase": "terminal",
            "request_id": attempt["driver_request_id"],
            "lease_fence": attempt["lease_fence"],
            "state": "cancelled",
        })
        return result

    # -- crash recovery ---------------------------------------------------------

    def recover(self, attempt: dict[str, Any]) -> dict[str, Any]:
        """Classify one attempt after a driver crash; observation only.

        Never spawns anything. Returns {"classification": ...} and applies
        exactly one state effect where evidence permits.
        """
        attempt_id = attempt["attempt_id"]
        status = self.runtime.attempt_status(attempt_id)
        if status["state"] in domain_runtime_module.TERMINAL_ATTEMPT_STATES:
            return {"classification": "already_terminal", "state": status["state"]}
        journal = self._read_journal(attempt_id)
        if journal is None:
            # No durable intent: nothing was ever spawned. Prove it with a
            # not_started receipt so the turns requeue safely.
            result = self._emit(
                attempt,
                state="not_started",
                error_code="never_spawned",
                incarnation="recovery:none",
            )
            return {"classification": "never_spawned", "state": result["state"]}
        phase = journal.get("phase")
        if phase in {"spawning", "spawned"}:
            # Proof of acceptance was lost after possible execution.
            if status["state"] == "prepared":
                self.runtime.mark_submitting(attempt_id)
            self.runtime.mark_uncertain(attempt_id, "spawn_proof_lost")
            return {"classification": "uncertain", "state": "uncertain"}
        if phase == "accepted":
            pid = int(journal.get("pid") or 0)
            recorded_start = journal.get("starttime")
            live_start = _process_starttime(pid) if pid else None
            if pid and recorded_start and live_start == recorded_start:
                return {"classification": "still_running", "state": status["state"]}
            # The exact process exited while we were away: the response file
            # is the durable outcome, so reconciliation is pure observation.
            return {
                "classification": "exited_reconciled",
                "state": self._terminal_receipt_from_exit(
                    attempt,
                    incarnation=f"{pid}:{recorded_start or 'unknown'}",
                    response_path=self._attempt_dir(attempt_id) / "response.out",
                    exit_code=0,
                )["state"],
            }
        if phase == "terminal":
            return {"classification": "already_terminal", "state": status["state"]}
        raise NativeDriverError("journal_corrupt")
