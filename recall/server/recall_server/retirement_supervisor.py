"""One bounded retirement child while the existing worker keeps projecting."""

from __future__ import annotations

import json
import logging
import signal
import subprocess
import sys
import time


LOG = logging.getLogger(__name__)


def _empty(status, *, unknown=False):
    return dict(
        status=status,
        cleared_documents=0,
        cleared_utf8_bytes=0,
        commit_outcome_unknown=unknown,
    )


def _launch(_configuration):
    return subprocess.Popen(
        [sys.executable, "-m", "recall_server.retirement_process"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )


class RetirementSupervisor:
    def __init__(self, dsn, *, tenant_id=None, seconds=180, launch=_launch, metadata_mode="keep"):
        if metadata_mode not in ("keep", "singleton"):
            raise ValueError("invalid retirement metadata mode")
        if type(seconds) not in (int, float) or not 0 < seconds <= 300:
            raise ValueError("invalid retirement work budget")
        self.configuration = dict(dsn=dsn, tenant_id=tenant_id, seconds=seconds)
        if metadata_mode != "keep":
            self.configuration["metadata_mode"] = metadata_mode
        self.launch, self.seconds = launch, seconds
        self.child = None
        self.started_at = 0.0
        self.closed = False
        self.previous_term = None

    def cycle(self, *, busy=False):
        if self.closed:
            return _empty("closed")
        if self.child is None:
            self.started_at = time.monotonic()
            self.child = self.launch(self.configuration)
            try:
                if self.child.stdin is not None and not self.child.stdin.closed:
                    self.child.stdin.write(json.dumps(self.configuration).encode())
                    self.child.stdin.close()
            except BaseException:
                self._terminate()
                raise
            return _empty("running")
        child = self.child
        if child.poll() is None:
            if time.monotonic() - self.started_at <= self.seconds + 10:
                return _empty("running")
            self._terminate()
            return _empty("commit_outcome_unknown", unknown=True)
        self.child = None
        try:
            payload = child.stdout.read(65537)
            if len(payload) > 65536:
                return _empty("commit_outcome_unknown", unknown=True)
            result = json.loads(payload)
            if not isinstance(result, dict) or not isinstance(
                result.get("status"), str
            ):
                return _empty("commit_outcome_unknown", unknown=True)
            for field in ("cleared_documents", "cleared_utf8_bytes"):
                if type(result.get(field)) is not int or result[field] < 0:
                    return _empty("commit_outcome_unknown", unknown=True)
            if child.returncode != 0:
                error_class = result.get("error_class")
                if (
                    result["status"] != "commit_outcome_unknown"
                    or result.get("commit_outcome_unknown") is not True
                    or result["cleared_documents"] != 0
                    or result["cleared_utf8_bytes"] != 0
                    or not isinstance(error_class, str)
                    or len(error_class) > 80
                    or not error_class.isidentifier()
                    or result.get("phase")
                    not in {
                        "configuration",
                        "database_setup",
                        "archive_setup",
                        "maintenance",
                        "shutdown",
                        "metrics",
                    }
                ):
                    return _empty("commit_outcome_unknown", unknown=True)
            LOG.info(
                "retirement_child status=%s elapsed_ms=%s cpu_seconds=%s peak_rss_kib=%s "
                "archive_bytes=%s cleared_utf8_bytes=%s unknown=%s error_class=%s phase=%s cause_error_code=%s "
                "completed_noop_parents=%s completed_parent_proofs=%s "
                "attempted_parents=%s failed_parents=%s errors=%r failed_phase_counts=%r failed_phase_ms=%r "
                "unknown_source_id=%r unknown_native_parent_id=%r unknown_scope_epoch=%s "
                "committed_sql_ms=%s committed_hash_ms=%s archive_read_ms=%s "
                "archive_reused_gets=%s archive_reused_bytes=%s "
                "claim_wall_ms=%s parent_wall_ms=%s metadata_capture_ms=%s retired_metadata_chunks=%s "
                "metadata_rows=%s metadata_fetches=%s metadata_fetch_ms=%s "
                "metadata_membership_calls=%s metadata_membership_ms=%s metadata_spool_ms=%s "
                "parent_thinned_documents=%s parent_thinned_events=%s "
                "parent_thin_document_bytes_removed=%s parent_thin_event_bytes_replaced=%s "
                "maintenance_phase=%s claim_phase=%s sqlstate=%s "
                "membership_certified_passages=%s membership_failed_parents=%s "
                "membership_deferred_parents=%s membership_elapsed_ms=%s "
                "audit_status=%s audit_cleared_rows=%s audit_scanned_rows=%s audit_archived_bytes=%s "
                "audit_reconciled_rows=%s audit_completed_pages=%s audit_unknown=%s audit_phase=%s audit_error_class=%s",
                result["status"],
                result.get("elapsed_ms"),
                result.get("cpu_seconds"),
                result.get("peak_rss_kib"),
                result.get("archive_bytes"),
                result["cleared_utf8_bytes"],
                result.get("commit_outcome_unknown"),
                result.get("error_class"),
                result.get("phase"),
                result.get("cause_error_code"),
                result.get("completed_noop_parents"),
                result.get("completed_parent_proofs"),
                result.get("attempted_parents"),
                result.get("failed_parents"),
                result.get("errors"),
                result.get("failed_phase_counts"),
                result.get("failed_phase_ms"),
                result.get("unknown_source_id"),
                result.get("unknown_native_parent_id"),
                result.get("unknown_scope_epoch"),
                result.get("committed_sql_ms"),
                result.get("committed_hash_ms"),
                result.get("archive_read_ms"),
                result.get("archive_reused_gets"),
                result.get("archive_reused_bytes"),
                result.get("claim_wall_ms"),
                result.get("parent_wall_ms"),
                result.get("metadata_capture_ms"),
                result.get("retired_metadata_chunks"),
                result.get("metadata_rows"),
                result.get("metadata_fetches"),
                result.get("metadata_fetch_ms"),
                result.get("metadata_membership_calls"),
                result.get("metadata_membership_ms"),
                result.get("metadata_spool_ms"),
                result.get("parent_thinned_documents"),
                result.get("parent_thinned_events"),
                result.get("parent_thin_document_bytes_removed"),
                result.get("parent_thin_event_bytes_replaced"),
                result.get("maintenance_phase"),
                result.get("claim_phase"),
                result.get("sqlstate"),
                result.get("membership_certified_passages"),
                result.get("membership_failed_parents"),
                result.get("membership_deferred_parents"),
                result.get("membership_elapsed_ms"),
                result.get("audit_status"),
                result.get("audit_cleared_rows"),
                result.get("audit_scanned_rows"),
                result.get("audit_archived_bytes"),
                result.get("audit_reconciled_rows"),
                result.get("audit_completed_pages"),
                result.get("audit_commit_outcome_unknown"),
                result.get("audit_phase"),
                result.get("audit_error_class"),

            )
            return result
        except (ValueError, OSError):
            return _empty("commit_outcome_unknown", unknown=True)
        finally:
            child.stdout.close()

    def _terminate(self):
        child = self.child
        if child is None:
            return
        try:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=2)
        finally:
            # Failed kill/wait must retain ownership: another cycle cannot launch
            # a second invocation while this exact process may still be alive.
            if child.poll() is not None:
                self.child = None
                if child.stdin is not None:
                    child.stdin.close()
                child.stdout.close()

    def close(self):
        self.closed = True
        self._terminate()

    def __enter__(self):
        self.previous_term = signal.getsignal(signal.SIGTERM)

        def stop(signum, frame):
            self.close()
            if callable(self.previous_term):
                self.previous_term(signum, frame)
            raise SystemExit(128 + signum)

        signal.signal(signal.SIGTERM, stop)
        return self

    def __exit__(self, *_args):
        try:
            self.close()
        finally:
            signal.signal(signal.SIGTERM, self.previous_term)
