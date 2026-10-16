#!/usr/bin/env python3
"""Logical and passage publication share one parent commit order."""

import threading
import time
import unittest
from unittest.mock import patch

import psycopg
from e2e_handback_projection import HandbackPublication


class PublicationParentLock(HandbackPublication):
    def test_concurrent_logical_and_passage_commit_preserves_newest_queue(self):
        self.ingest("event-one", text="First genuine visible finding.")
        self.logical.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1
        )
        passage = self.passages._prepare(
            self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        )
        self.ingest(
            "event-two",
            text="Second newer visible finding.",
            occurred="2026-10-13T12:00:01Z",
        )
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        (upload,) = self.logical._prepare_batch_and_upload((candidate,))
        parent_locked, release_parent = threading.Event(), threading.Event()
        passage_started, passage_queue_locked = threading.Event(), threading.Event()
        logical_queue_attempt = threading.Event()
        outcomes, errors, passage_pid = {}, {}, []
        execute = psycopg.Connection.execute

        def traced(connection, query, *args, **kwargs):
            sql = str(query)
            thread = threading.current_thread().name
            if (
                thread == "logical-owner"
                and "INSERT INTO canonical_passage_projection_queue(" in sql
            ):
                logical_queue_attempt.set()
            if thread == "passage-owner" and "pg_advisory_xact_lock" in sql:
                if not passage_pid:
                    passage_pid.append(connection.info.backend_pid)
                    passage_started.set()
            cursor = execute(connection, query, *args, **kwargs)
            if (
                thread == "logical-owner"
                and "INSERT INTO canonical_evidence_documents(" in sql
            ):
                parent_locked.set()
                if not release_parent.wait(5):
                    raise AssertionError("parent interleave timeout")
            if (
                thread == "passage-owner"
                and "FROM canonical_passage_projection_queue" in sql
                and "FOR UPDATE" in sql
            ):
                passage_queue_locked.set()
                if not logical_queue_attempt.wait(5):
                    raise AssertionError("logical queue interleave timeout")
            return cursor

        def run(name, callback):
            try:
                outcomes[name] = callback()
            except BaseException as error:
                errors[name] = error

        logical = threading.Thread(
            name="logical-owner",
            target=run,
            args=("logical", lambda: self.logical._commit_upload(candidate, upload)),
        )
        passages = threading.Thread(
            name="passage-owner",
            target=run,
            args=("passages", lambda: self.passages._commit(passage)),
        )
        with patch.object(psycopg.Connection, "execute", traced):
            try:
                logical.start()
                self.assertTrue(parent_locked.wait(5))
                passages.start()
                self.assertTrue(passage_started.wait(5))
                deadline = time.monotonic() + 5
                waiting_on_parent = False
                while not passage_queue_locked.is_set() and time.monotonic() < deadline:
                    with self.store.connect() as connection:
                        row = connection.execute(
                            "SELECT wait_event FROM pg_stat_activity WHERE pid=%s",
                            (passage_pid[0],),
                        ).fetchone()
                    if row and row["wait_event"] == "advisory":
                        waiting_on_parent = True
                        break
                    time.sleep(0.01)
                self.assertTrue(passage_queue_locked.is_set() or waiting_on_parent)
                release_parent.set()
                logical.join(8)
                passages.join(8)
            finally:
                release_parent.set()
                logical_queue_attempt.set()
                if logical.ident is not None:
                    logical.join(8)
                if passages.ident is not None:
                    passages.join(8)
        self.assertFalse(logical.is_alive() or passages.is_alive())
        self.assertEqual(
            {name: type(error).__name__ for name, error in errors.items()},
            {},
            "actual native commit paths must not deadlock",
        )
        self.assertEqual(outcomes["logical"], "committed")
        self.assertEqual(outcomes["passages"]["status"], "stale")
        queued = self.passages._pending(tenant_id=self.tenant, limit=1)
        self.assertEqual(queued[0].revision, candidate.revision)
        self.assertEqual(
            self.passages._commit(self.passages._prepare(queued[0]))["status"],
            "committed",
        )
        text = " ".join(row["text_redacted"] for row in self.projected())
        self.assertIn("First genuine visible finding.", text)
        self.assertIn("Second newer visible finding.", text)

    def test_unrelated_parent_commits_while_same_parent_waits(self):
        from recall_server.logical_evidence import logical_document_id

        self.ingest("event-one", text="First independent parent.")
        original = self.plane.ingest_document

        def other_parent(*args, **kwargs):
            kwargs["envelope"] = dict(
                kwargs["envelope"], native_parent_id="session-two"
            )
            return original(*args, **kwargs)

        with patch.object(self.plane, "ingest_document", side_effect=other_parent):
            self.ingest("event-other", text="Second independent parent.")
        self.logical.project_pending(
            tenant_id=self.tenant, batch_size=2, max_batches=1, upload_concurrency=1
        )
        candidates = self.passages._pending(tenant_id=self.tenant, limit=2)
        prepared = {
            row.logical_document_id: self.passages._prepare(row) for row in candidates
        }
        first = logical_document_id(self.tenant, self.source, "session-one")
        second = logical_document_id(self.tenant, self.source, "session-two")
        outcomes, errors, pid = {}, {}, []
        entered = threading.Event()
        execute = psycopg.Connection.execute

        def traced(connection, query, *args, **kwargs):
            if (
                threading.current_thread().name == "blocked-parent"
                and "pg_advisory_xact_lock" in str(query)
            ):
                if not pid:
                    pid.append(connection.info.backend_pid)
                    entered.set()
            return execute(connection, query, *args, **kwargs)

        def commit():
            try:
                outcomes["first"] = self.passages._commit(prepared[first])
            except BaseException as error:
                errors["first"] = error

        thread = threading.Thread(target=commit, name="blocked-parent")
        with patch.object(psycopg.Connection, "execute", traced):
            try:
                with self.store.connect() as connection, connection.transaction():
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        ("logical-evidence\x1f" + first,),
                    )
                    thread.start()
                    self.assertTrue(entered.wait(5))
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        with self.store.connect() as observer:
                            state = observer.execute(
                                "SELECT wait_event FROM pg_stat_activity WHERE pid=%s",
                                (pid[0],),
                            ).fetchone()
                        if state and state["wait_event"] == "advisory":
                            break
                        time.sleep(0.01)
                    else:
                        self.fail("same parent did not wait on its logical commit lock")
                    outcomes["second"] = self.passages._commit(prepared[second])
                    self.assertEqual(outcomes["second"]["status"], "committed")
                    self.assertNotIn("first", outcomes)
            finally:
                if thread.ident is not None:
                    thread.join(8)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, {})
        self.assertEqual(outcomes["first"]["status"], "committed")


if __name__ == "__main__":
    suite = unittest.TestSuite(
        PublicationParentLock(name)
        for name in PublicationParentLock.__dict__
        if name.startswith("test_")
    )
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
