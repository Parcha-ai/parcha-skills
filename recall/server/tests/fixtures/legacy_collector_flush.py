"""Frozen pre-retention collector flush; protocol regression only.
Source commit: babf6c043d4a6d37b7c57105a5d42d722292e259
Method SHA256: 5718fa614f59eb10b26bfd7ae6b36233c39f9dc6c3824169bc5829b8576ac59e
"""
import hashlib
import json
import time
import urllib.error
import urllib.request
from collector.collector import canonical_json, MAX_CANONICAL_TOMBSTONE_BATCH_EVENTS
from privacy.transport import open_no_redirect, retry_delay_seconds

def flush(self) -> dict:
    recovery = self.recover_dead_payloads() if self.shard_index == 0 else {"recovered": 0, "unrecoverable": 0}
    result = {"batches": 0, "acked": 0, "replayed_batches": 0, "errors": 0, **recovery}
    max_batch_bytes = self._max_batch_bytes()
    while True:
        raw_candidates = list(self.db.execute(
            "SELECT * FROM outbox WHERE state='pending' AND (shard_key % ?) = ? ORDER BY id LIMIT ?",
            (self.shard_count, self.shard_index, self.batch_size),
        ))
        if not raw_candidates:
            break
        candidates = []
        for row in raw_candidates:
            candidate = self._repair_pending_envelope(row)
            if candidate is not None:
                candidates.append(candidate)
        self.db.commit()
        rows: list[dict] = []
        batch_kind: str | None = None
        live_native_ids: set[str] = set()
        body_size = len(b'{"events":[]}')
        for candidate in candidates:
            envelope = json.loads(candidate["envelope_json"])
            kind = envelope.get("kind")
            candidate_batch_kind = (
                "tombstone" if kind == "tombstone" else "live"
            )
            # The canonical server commits ordinary live records through a
            # set-based transaction. Tombstones intentionally use the
            # lineage-aware path on older compatible servers, so keep
            # those requests small enough that a retry cannot overlap a
            # still-running transaction. Never mix the two paths in one
            # request.
            if batch_kind is not None and candidate_batch_kind != batch_kind:
                break
            if (
                candidate_batch_kind == "tombstone"
                and len(rows) >= MAX_CANONICAL_TOMBSTONE_BATCH_EVENTS
            ):
                break
            native_id = envelope.get("native_id")
            if candidate_batch_kind == "live" and native_id in live_native_ids:
                # Multiple revisions for one native identity require the
                # sequential lineage path. Commit the earlier revision
                # first so each request remains eligible for set-based SQL.
                break
            event_size = len(candidate["envelope_json"].encode()) + 1
            if not rows and event_size > max_batch_bytes:
                with self.db:
                    self.db.execute("UPDATE outbox SET state='dead',envelope_json='{}' WHERE id=?", (candidate["id"],))
                    self.db.execute(
                        "INSERT OR IGNORE INTO dead_letters(path,byte_offset,error_code,error_summary,created_at) VALUES (?,?,?,?,?)",
                        (candidate["path"], candidate["start_offset"], "PayloadTooLarge", f"sanitized envelope exceeds {max_batch_bytes} bytes", time.time()),
                    )
                result["errors"] += 1
                continue
            if rows and body_size + event_size > max_batch_bytes:
                break
            batch_kind = candidate_batch_kind
            if candidate_batch_kind == "live" and isinstance(native_id, str):
                live_native_ids.add(native_id)
            rows.append(candidate)
            body_size += event_size
        if not rows:
            continue
        events = [json.loads(row["envelope_json"]) for row in rows]
        key_material = self.source_id + ":" + ",".join(f"{row['id']}:{row['content_sha256']}" for row in rows)
        key = "collector-v1-" + hashlib.sha256(key_material.encode()).hexdigest()
        body = canonical_json({"events": events})
        request = urllib.request.Request(
            self.endpoint + "/v1/ingest/batches", data=body, method="POST",
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json", "Idempotency-Key": key},
        )
        acknowledgement = None
        last_error = None
        for attempt in range(5):
            last_error = None
            try:
                if self.brain_writer is not None:
                    acknowledgement = self.brain_writer.ingest(events)
                else:
                    with open_no_redirect(request, timeout=60) as response:
                        acknowledgement = json.loads(response.read())
                        if response.status not in {200, 201}:
                            raise RuntimeError(
                                "server did not return a commit acknowledgement"
                            )
                if acknowledgement.get("status") != "committed":
                    raise RuntimeError(
                        "server did not return a commit acknowledgement"
                    )
                break
            except urllib.error.HTTPError as exc:
                result["errors"] += 1
                if exc.code < 500:
                    self._record_error(
                        "brain_unauthorized"
                        if exc.code in {401, 403}
                        else "brain_rejected"
                    )
                    return result
                last_error = exc
                self._record_error("brain_unavailable")
            except PermissionError:
                result["errors"] += 1
                self._record_error("brain_unauthorized")
                return result
            except ValueError:
                result["errors"] += 1
                self._record_error("brain_rejected")
                return result
            except (OSError, urllib.error.URLError):
                result["errors"] += 1
                self._record_error("brain_unavailable")
            except (json.JSONDecodeError, RuntimeError):
                result["errors"] += 1
                self._record_error("brain_invalid_acknowledgement")
                return result
            except Exception:
                result["errors"] += 1
                self._record_error("brain_unavailable")
            if attempt < 4:
                time.sleep(retry_delay_seconds(last_error, attempt, base_cap=10))
        if acknowledgement is None:
            return result
        receipts = acknowledgement.get("receipts", [])
        if len(receipts) != len(rows):
            result["errors"] += 1
            self._record_error("brain_invalid_acknowledgement")
            break
        self._after_remote_commit(acknowledgement)
        acked_at = time.time()
        with self.db:
            acknowledgements = list(zip(rows, receipts, strict=True))
            self.db.executemany(
                "UPDATE outbox SET state='acked',acked_at=?,receipt=?,envelope_json='{}' WHERE id=?",
                [(acked_at, receipt, row["id"]) for row, receipt in acknowledgements],
            )
            self.db.executemany(
                "UPDATE active_records SET receipt=? WHERE path=? AND native_id=?",
                [(receipt, row["path"], row["native_id"]) for row, receipt in acknowledgements],
            )
            self.db.executemany(
                "DELETE FROM dead_letters "
                "WHERE path=? AND byte_offset=? AND error_code='RecoveryError'",
                [(row["path"], row["start_offset"]) for row in rows],
            )
            for path in {row["path"] for row in rows}:
                pending = self.db.execute("SELECT 1 FROM outbox WHERE path=? AND state='pending' LIMIT 1", (path,)).fetchone()
                if not pending:
                    self.db.execute("UPDATE files SET committed_offset=scanned_offset WHERE path=?", (path,))
            self._set_meta("last_success_epoch", str(int(acked_at)))
            self._clear_error()
        result["batches"] += 1
        result["acked"] += len(rows)
        result["replayed_batches"] += int(bool(acknowledgement.get("replay")))
    return result
