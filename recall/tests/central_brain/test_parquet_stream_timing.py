"""Attribute unfinished scan work without changing the publication owner."""
import unittest
from unittest import mock

from recall_server import parquet_scan as scan
from tests.central_brain.test_parquet_scan import (
    _DocumentArchive, _FragmentProbe, _candidate, _month_document,
)


class Clock:
    now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def counters(call):
    message = call.args[0] % call.args[1:]
    return {key: int(value) for key, value in
            (field.split("=") for field in message.split()[3:])}


class StreamTimingTests(unittest.TestCase):
    def test_unfinished_passage_work_and_overlapping_owner_read_are_distinct(self):
        clock = Clock()
        progress_seen = []
        archive = _DocumentArchive({"document:0": 1})

        class Probe(_FragmentProbe):
            def _preserve_rows(self, *args):
                clock.advance(1)
                return None, None

            def _passages(self, *args):
                clock.advance(6)
                yield from ()

            def _project_document(self, *args, **kwargs):
                progress_seen.extend(new_logs(log))
                clock.advance(3)
                return super()._project_document(*args, **kwargs)

        class Reads:
            def __init__(self, archive, parts):
                self.archive = archive
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def __call__(self, part):
                clock.advance(2)
                return self.archive.read_raw(scan._reference(part))

        probe = Probe([_month_document("document:0")],
                      scan.ScanCatalog({}, {}, frozenset()), archive)
        with mock.patch.object(scan.time, "perf_counter", clock), \
             mock.patch.object(scan, "_PartReadAhead", Reads), \
             mock.patch.object(scan.LOG, "info") as log:
            result = probe._build(_candidate())
        self.assertTrue(progress_seen, "unfinished passage stage was invisible")
        first = counters(progress_seen[0])
        self.assertEqual(first["finished"], 0)
        self.assertEqual(first["passage_ms"], 6000)
        final = counters(new_logs(log)[-1])
        self.assertEqual(final["finished"], 1)
        self.assertEqual(final["preserve_ms"], 1000)
        self.assertEqual(final["read_ms"], 2000)
        self.assertEqual(final["project_ms"], 5000)
        self.assertEqual(final["elapsed_ms"], 12000)
        self.assertTrue(result.created)
        self.assertEqual(len(archive.reads), 1)

    def test_fast_many_documents_emit_only_final_aggregate(self):
        archive = _DocumentArchive({f"document:{i}": 1 for i in range(100)})
        probe = _FragmentProbe([_month_document(k) for k in archive.records],
                               scan.ScanCatalog({}, {}, frozenset()), archive)
        with mock.patch.object(scan.time, "perf_counter", return_value=0), \
             mock.patch.object(scan.LOG, "info") as log:
            probe._build(_candidate())
        messages = new_logs(log)
        self.assertEqual(len(messages), 1)
        self.assertEqual(counters(messages[0])["read_calls"], 100)
        self.assertEqual(counters(messages[0])["project_calls"], 100)
        self.assertEqual(counters(messages[0])["failed"], 0)
        self.assertNotIn("document:", str(messages))
        self.assertNotIn("recall://", str(messages))

    def test_original_error_and_cleanup_survive_diagnostic_failure(self):
        problem = RuntimeError("original private provider failure")
        archive = _DocumentArchive({"document:0": 1})
        probe = _FragmentProbe([_month_document("document:0")],
                               scan.ScanCatalog({}, {}, frozenset()), archive)
        with mock.patch.object(probe, "_preserve_rows", side_effect=problem), \
             mock.patch.object(scan.LOG, "info", side_effect=lambda msg, *args:
                               fail_log(msg)), \
             mock.patch.object(scan._StreamingUpload, "abort") as abort:
            with self.assertRaises(RuntimeError) as raised:
                probe._build(_candidate())
        self.assertIs(raised.exception, problem)
        abort.assert_called_once()

    def test_passage_advancement_excludes_sinks_and_final_upload_is_existing_counter(self):
        from tests.central_brain.test_parquet_cross_dataset_delta import Probe
        clock = Clock()

        class TimedProbe(Probe):
            def _passages(self, *args):
                for row in super()._passages(*args):
                    clock.advance(7)
                    yield row
                clock.advance(1)

        archive = _DocumentArchive({"document:0": 1})
        probe = TimedProbe([_month_document("document:0")],
                          scan.ScanCatalog({}, {}, frozenset()), archive)
        original_add = scan._StreamingUpload.add
        original_put = archive.put_raw

        def add(upload, dataset, *args, **kwargs):
            if dataset == "passages":
                clock.advance(11)
            return original_add(upload, dataset, *args, **kwargs)

        def put(**kwargs):
            clock.advance(2)
            return original_put(**kwargs)

        with mock.patch.object(scan.time, "perf_counter", clock),              mock.patch.object(scan._StreamingUpload, "add", add),              mock.patch.object(archive, "put_raw", put),              mock.patch.object(scan.LOG, "info") as log:
            result = probe._build(_candidate())
        final = counters(new_logs(log)[-1])
        self.assertEqual(final["passage_ms"], 8000)
        self.assertEqual(final["passage_calls"], 2)
        self.assertEqual(final["upload_ms"], len(archive.uploads) * 2000)
        self.assertGreater(final["elapsed_ms"], final["passage_ms"] + final["upload_ms"])
        self.assertEqual(result.row_counts[("passages", 0)], 1)

    def test_failed_iterator_reports_failure_without_error_prose_or_output(self):
        problem = RuntimeError("secret SQL detail")
        archive = _DocumentArchive({"document:0": 1})
        probe = _FragmentProbe([_month_document("document:0")],
                               scan.ScanCatalog({}, {}, frozenset()), archive)

        def broken(*_):
            raise problem
            yield

        with mock.patch.object(probe, "_passages", broken),              mock.patch.object(scan.LOG, "info") as log,              mock.patch.object(scan._StreamingUpload, "abort") as abort:
            with self.assertRaises(RuntimeError) as raised:
                probe._build(_candidate())
        self.assertIs(raised.exception, problem)
        self.assertEqual(counters(new_logs(log)[-1])["failed"], 1)
        self.assertNotIn("secret", str(new_logs(log)))
        abort.assert_called_once()
        self.assertEqual(archive.uploads, [])

    def test_diagnostic_failure_does_not_change_success(self):
        archive = _DocumentArchive({"document:0": 1})
        probe = _FragmentProbe([_month_document("document:0")],
                               scan.ScanCatalog({}, {}, frozenset()), archive)
        with mock.patch.object(scan.LOG, "info", side_effect=lambda msg, *args:
                               fail_log(msg)):
            result = probe._build(_candidate())
        self.assertTrue(result.created)
        self.assertEqual(result.row_counts[("records", 0)], 1)

    def test_failed_derived_attempt_finishes_before_recursive_recovery(self):
        clock = Clock()
        archive = _DocumentArchive({"document:0": 1})
        probe = _FragmentProbe([_month_document("document:0")],
                               scan.ScanCatalog({}, {}, frozenset()), archive)
        attempts = []

        def preserve(*_):
            attempts.append(True)
            if len(attempts) == 1:
                clock.advance(5)
                raise scan._DerivedFragmentUnavailable("unavailable")
            clock.advance(11)
            return None, None

        with mock.patch.object(scan.time, "perf_counter", clock),              mock.patch.object(probe, "_preserve_rows", preserve),              mock.patch.object(scan._StreamingUpload, "abort") as abort,              mock.patch.object(scan.LOG, "info") as log:
            result = probe._build(_candidate())
        finals = [counters(row) for row in new_logs(log)
                  if counters(row)["finished"]]
        self.assertEqual([(row["failed"], row["elapsed_ms"]) for row in finals],
                         [(1, 5000), (0, 11000)])
        self.assertTrue(result.created)
        abort.assert_called_once()


def fail_log(message):
    if message.startswith("parquet stage timing"):
        raise OSError("log unavailable")


def new_logs(log):
    return [call for call in log.call_args_list
            if call.args[0].startswith("parquet stage timing")]
