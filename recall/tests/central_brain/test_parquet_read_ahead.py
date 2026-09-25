"""Archive latency overlap without changing the sequential Parquet owner."""

import copy
import gc
import hashlib
import weakref
from unittest import mock
from datetime import datetime, timezone
import threading
import unittest

from recall_server.parquet_scan import ScanCatalog
from tests.central_brain.test_parquet_scan import (
    _DocumentArchive,
    _FragmentProbe,
    _candidate,
    _month_document,
)


class ReadAheadTests(unittest.TestCase):
    def test_reads_overlap_across_tiny_one_part_documents(self):
        second_started = threading.Event()
        witnessed = []
        owner = threading.get_ident()

        class Archive(_DocumentArchive):
            def read_raw(self, reference):
                name = reference["artifact_id"].split(":", 1)[1]
                if name == "document:0":
                    witnessed.append(second_started.wait(0.5))
                elif name == "document:1":
                    second_started.set()
                return super().read_raw(reference)

            def put_raw(self, **kwargs):
                self_owner = threading.get_ident()
                assert self_owner == owner, "output publication escaped owner thread"
                return super().put_raw(**kwargs)

        archive = Archive({f"document:{i}": 1 for i in range(12)})
        documents = [_month_document(name) for name in archive.records]
        for document in documents:
            part = document["parts"][0]
            payload = _DocumentArchive.read_raw(archive, part)
            part.update(
                size_bytes=len(payload),
                content_sha256=hashlib.sha256(payload).hexdigest(),
            )
        archive.reads.clear()
        scan = _FragmentProbe(documents, ScanCatalog({}, {}, frozenset()), archive)
        result = scan._build(_candidate())
        self.assertEqual(
            witnessed, [True], "second tiny document GET did not overlap first"
        )
        self.assertEqual(len(archive.reads), 12)
        self.assertEqual(
            sum(
                n
                for (dataset, _), n in result.row_counts.items()
                if dataset == "records"
            ),
            12,
        )

    def test_actual_parquet_rows_members_and_bounds_equal_sequential(self):
        from recall_server.parquet_scan import _reference
        from tests.central_brain.test_parquet_cross_dataset_delta import (
            Archive,
            Probe,
            decode,
        )

        class Serial:
            def __init__(self, archive, _parts):
                self.archive = archive

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def __call__(self, part):
                return self.archive.read_raw(_reference(part))

        class BoundsProbe(Probe):
            def _persist_part_bounds(self, bounds):
                self.seen_bounds.extend(bounds)

        documents = [_month_document(f"document:{i}") for i in range(12)]
        for document in documents:
            document["actor_links"] = [
                dict(actor_id="actor:test", display_name="A", relation="speaker")
            ]
            document["parts"][0].update(first_occurred_at=None, last_occurred_at=None)
        outputs = []
        for serial in (True, False):
            archive = Archive({d["logical_document_id"]: 7 for d in documents})
            docs = copy.deepcopy(documents)
            for doc in docs:
                payload = archive.read_raw(doc["parts"][0])
                doc["parts"][0].update(
                    size_bytes=len(payload),
                    content_sha256=hashlib.sha256(payload).hexdigest(),
                )
            archive.reads.clear()
            probe = BoundsProbe(docs, ScanCatalog({}, {}, frozenset()), archive)
            probe.seen_bounds = []
            context = (
                mock.patch("recall_server.parquet_scan._PartReadAhead", Serial)
                if serial
                else mock.patch("recall_server.parquet_scan.PART_READ_AHEAD_TASKS", 8)
            )
            with (
                context,
                mock.patch("recall_server.parquet_scan.PARQUET_RAW_SLICE_BYTES", 2048),
            ):
                result = probe._build(_candidate())
            rows = {
                dataset: []
                for dataset in ("documents", "records", "actors", "passages")
            }
            for (dataset, _), reference in result.references.items():
                rows[dataset].extend(decode(archive.read_raw(reference)))
            self.assertTrue(all(rows.values()))
            outputs.append((rows, result.members, result.bounds, probe.seen_bounds))
        self.assertEqual(outputs[0], outputs[1])

    def test_completed_waiting_bodies_and_tasks_remain_bounded_and_released(self):
        from recall_server.parquet_scan import _PartReadAhead

        release = threading.Event()
        waiting = threading.Event()
        finished = []
        refs = []

        class Body:
            def __init__(self):
                self.data = bytearray(1024 * 1024)

        class Archive:
            def read_raw(self, part):
                if part["artifact_id"] == "raw:document:0":
                    if not release.wait(2):
                        raise AssertionError("test release missing")
                value = Body()
                refs.append(weakref.ref(value))
                finished.append(part["artifact_id"])
                if len(finished) == 7:
                    waiting.set()
                return value

        parts = [_month_document(f"document:{i}")["parts"][0] for i in range(20)]
        for part in parts:
            part["size_bytes"] = 1024 * 1024
        reader = _PartReadAhead(Archive(), parts)
        errors = []

        def consume():
            try:
                with reader:
                    body = reader(parts[0])
                    del body
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=consume)
        thread.start()
        try:
            self.assertTrue(waiting.wait(2))
            self.assertEqual(len(finished), 7)
            self.assertEqual(reader.reserved_bytes, 8 * 1024 * 1024)
            self.assertEqual(len(reader.pending), 7)
            self.assertEqual(sum(r() is not None for r in refs), 7)
        finally:
            release.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        gc.collect()
        self.assertTrue(all(r() is None for r in refs))
        self.assertEqual(reader.reserved_bytes, 0)

    def test_oversized_part_uses_original_owner_path_after_drain(self):
        from recall_server.parquet_scan import _PartReadAhead

        owner = threading.get_ident()
        reads = []

        class Archive:
            def read_raw(self, part):
                reads.append((part["artifact_id"], threading.get_ident()))
                return b"body"

        parts = [_month_document(f"document:{i}")["parts"][0] for i in range(3)]
        parts[1]["size_bytes"] = 128 * 1024 * 1024
        with _PartReadAhead(Archive(), parts) as reader:
            reader(parts[0])
            self.assertEqual([r[0] for r in reads], [parts[0]["artifact_id"]])
            reader(parts[1])
            self.assertEqual(reads[-1], (parts[1]["artifact_id"], owner))
            self.assertEqual(len(reads), 2)
            reader(parts[2])
        self.assertEqual(len(reads), 3)

    def test_missing_future_requeues_only_its_document_on_owner_thread(self):
        from recall_server.archive import ArchiveNotFound
        from recall_server.parquet_scan import ParquetScanError
        from tests.central_brain.test_parquet_cross_dataset_delta import Probe

        owner = threading.get_ident()
        requeued = []
        decoded = []

        class Archive(_DocumentArchive):
            def read_raw(self, part):
                if part["artifact_id"] == "raw:document:2":
                    raise ArchiveNotFound("missing")
                return super().read_raw(part)

        class Owner(Probe):
            def _requeue_missing_document(self, document):
                requeued.append(
                    (document["logical_document_id"], threading.get_ident())
                )

            def _project_document(self, candidate, document, **kwargs):
                decoded.append(document["logical_document_id"])
                return super()._project_document(candidate, document, **kwargs)

        docs = [_month_document(f"document:{i}") for i in range(10)]
        archive = Archive({d["logical_document_id"]: 1 for d in docs})
        with self.assertRaisesRegex(ParquetScanError, "evidence_requeued"):
            Owner(docs, ScanCatalog({}, {}, frozenset()), archive)._build(_candidate())
        self.assertEqual(requeued, [("document:2", owner)])
        self.assertEqual(decoded, ["document:0", "document:1", "document:2"])
        self.assertEqual(len(archive.reads), len(set(archive.reads)))

    def test_skipped_and_invalid_later_bounds_do_not_move_failure_earlier(self):
        from recall_server.parquet_scan import ParquetScanError
        from tests.central_brain.test_parquet_cross_dataset_delta import Probe

        docs = [_month_document(f"document:{i}") for i in range(4)]
        docs[1]["parts"][0].update(
            first_occurred_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            last_occurred_at=datetime(2026, 7, 2, tzinfo=timezone.utc),
        )
        docs[2]["parts"][0]["first_occurred_at"] = None
        decoded = []

        class Owner(Probe):
            def _project_document(self, candidate, document, **kwargs):
                decoded.append(document["logical_document_id"])
                return super()._project_document(candidate, document, **kwargs)

        archive = _DocumentArchive({d["logical_document_id"]: 1 for d in docs})
        with self.assertRaisesRegex(ParquetScanError, "state_invalid"):
            Owner(docs, ScanCatalog({}, {}, frozenset()), archive)._build(_candidate())
        self.assertEqual(archive.reads, ["document:0"])
        self.assertEqual(decoded, ["document:0", "document:1", "document:2"])

    def test_later_invalid_reference_is_consumed_after_prior_documents(self):
        from tests.central_brain.test_parquet_cross_dataset_delta import Probe

        for field in ("storage_backend", "size_bytes"):
            with self.subTest(field=field):
                docs = [_month_document(f"document:{i}") for i in range(3)]
                del docs[1]["parts"][0][field]
                decoded = []

                class Owner(Probe):
                    def _project_document(self, candidate, document, **kwargs):
                        decoded.append(document["logical_document_id"])
                        return super()._project_document(candidate, document, **kwargs)

                archive = _DocumentArchive({d["logical_document_id"]: 1 for d in docs})
                with self.assertRaises(KeyError):
                    Owner(docs, ScanCatalog({}, {}, frozenset()), archive)._build(
                        _candidate()
                    )
                self.assertEqual(decoded, ["document:0", "document:1"])
                self.assertIn("document:0", archive.reads)

    def test_consumer_interrupt_settles_reads_and_cancels_unstarted_tasks(self):
        from concurrent.futures import ThreadPoolExecutor
        from recall_server.parquet_scan import _PartReadAhead

        reads = []

        class Archive:
            def read_raw(self, part):
                reads.append(part["artifact_id"])
                return b"body"

        parts = [_month_document(f"document:{i}")["parts"][0] for i in range(100)]
        with mock.patch(
            "recall_server.parquet_scan.ThreadPoolExecutor",
            side_effect=lambda **_: ThreadPoolExecutor(max_workers=1),
        ):
            reader = _PartReadAhead(Archive(), parts)
            with self.assertRaises(KeyboardInterrupt):
                with reader:
                    reader(parts[0])
                    raise KeyboardInterrupt
        self.assertLessEqual(len(reads), 8)
        self.assertEqual(reader.pending, __import__("collections").deque())
        self.assertTrue(
            all(not thread.is_alive() for thread in reader.executor._threads)
        )


if __name__ == "__main__":
    unittest.main()
