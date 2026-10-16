# ruff: noqa: E402  # Direct scripts add the repository modules to sys.path.
"""Remote launch intent is durable before one POST; uncertain jobs never retry."""

from pathlib import Path
import json
import urllib.parse
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
from recall_server.retirement_jobs import (
    RenderJob,
    encode_result,
    decode_result,
    reconcile_enrollment,
    package_body,
)
from recall_server.retirement_campaign import digest


class JobTests(unittest.TestCase):
    def test_original_job_body_must_match_durable_launch(self):
        import base64
        import gzip
        import hashlib
        from recall_server.retirement_jobs import verify_original_job

        spec = dict(payload=dict(operation="enroll", plans=[]))
        pin = digest(spec)
        body = "SPEC=" + repr(spec) + "\nPIN=" + repr(pin) + "\n"
        command = (
            'python -u -c exec(__import__("gzip").decompress(__import__("base64").b64decode("'
            + base64.b64encode(gzip.compress(body.encode())).decode()
            + '")))'
        )
        config = dict(service_id="service")
        launch = dict(
            config_sha256=digest(config),
            pin=pin,
            body_sha256=hashlib.sha256(body.encode()).hexdigest(),
        )
        events = [
            dict(name="launch_intent", value=launch),
            dict(name="job", value=dict(id="job")),
        ]
        get = Mock(
            return_value=dict(id="job", status="succeeded", startCommand=command)
        )
        with patch("recall_server.retirement_jobs.request", get):
            proof = verify_original_job(config, "job", [], events)
            self.assertEqual(proof["body_sha256"], launch["body_sha256"])
            self.assertEqual(get.call_args.args[0], "GET")
            get.return_value["startCommand"] = command.replace("python -u", "python")
            with self.assertRaisesRegex(ValueError, "original_job_command"):
                verify_original_job(config, "job", [], events)
            get.return_value["startCommand"] = command
            launch["body_sha256"] = "wrong"
            with self.assertRaisesRegex(ValueError, "original_job_body"):
                verify_original_job(config, "job", [], events)

    def test_observation_never_replays_enrollment_and_keeps_attempts_distinct(self):
        from recall_server.retirement_jobs import reconcile_observed_enrollment
        from recall_server import retirement_jobs as jobs
        import ast

        config = dict(
            commit="current",
            service_id="service",
            owner_modules={
                "recall_server." + name: "0" * 64
                for name in (
                    "retirement_runner",
                    "chunk_retirement",
                    "parent_chunk_proof",
                    "chunk_bodies",
                    "streaming_locators",
                    "locator_backfill_plan",
                )
            },
        )
        emitted = []

        def run(body, pin, emit):
            spec = ast.literal_eval(body.splitlines()[0][5:])
            self.assertEqual(spec["payload"], dict(operation="observe", plans=[]))
            emit("launch_intent", dict(pin=pin))
            emit("job", dict(id="read-job"))
            return dict(read_only=True, archive_calls=0, parents=[])

        with (
            patch.object(
                jobs, "verify_original_job", return_value=dict(id="write-job")
            ),
            patch.object(RenderJob, "run", side_effect=run),
        ):
            result = reconcile_observed_enrollment(
                config, config, "write-job", [], [], lambda *e: emitted.append(e)
            )
        self.assertEqual(result["original_job"]["id"], "write-job")
        self.assertEqual(result["observation_job"]["id"], "read-job")
        self.assertEqual(
            [e[0] for e in emitted],
            [
                "original_job_body_verified",
                "observation_launch_intent",
                "observation_job",
            ],
        )
        with (
            patch.object(
                jobs, "verify_original_job", side_effect=ValueError("original_job_body")
            ),
            patch.object(RenderJob, "run") as run,
        ):
            with self.assertRaisesRegex(ValueError, "original_job_body"):
                reconcile_observed_enrollment(
                    config, config, "write-job", [], [], Mock()
                )
            run.assert_not_called()

    def test_identical_plans_cannot_reconcile_an_older_attempt(self):
        config = dict(
            commit="commit",
            service_id="service",
            owner_modules={
                "recall_server." + name: "0" * 64
                for name in (
                    "retirement_runner",
                    "chunk_retirement",
                    "parent_chunk_proof",
                    "chunk_bodies",
                    "streaming_locators",
                    "locator_backfill_plan",
                )
            },
        )
        _, older_pin = package_body(config, dict(operation="enroll", plans=[]))
        _, current_pin = package_body(config, dict(operation="enroll", plans=[]))
        self.assertNotEqual(older_pin, current_pin)
        with self.assertRaisesRegex(ValueError, "wrong_pin"):
            decode_result(encode_result([], older_pin), current_pin)

    def test_reconciliation_uses_original_pin_config_and_recorded_job(self):
        config = dict(service_id="service")
        launch = dict(
            pin="original-pin", config_sha256=digest(config), owner_id="owner"
        )
        events = [
            dict(name="launch_intent", value=launch),
            dict(name="job", value=dict(id="job")),
        ]
        with patch.object(RenderJob, "reconcile", return_value=[]) as inspect:
            self.assertEqual(reconcile_enrollment(config, "job", events, Mock()), [])
            self.assertEqual(
                inspect.call_args.args[:3], ("job", "original-pin", "owner")
            )
            inspect.reset_mock()
            with self.assertRaisesRegex(ValueError, "reconciliation_job_changed"):
                reconcile_enrollment(config, "wrong", events, Mock())
            inspect.assert_not_called()
            with self.assertRaisesRegex(ValueError, "reconciliation_config_changed"):
                reconcile_enrollment(dict(service_id="other"), "job", events, Mock())
            inspect.assert_not_called()

    def test_reconcile_reads_exact_terminal_job_without_launch(self):
        outcome = [dict(status="enrolled", certainty="known")]
        calls = []

        def request(method, path, body=None):
            calls.append((method, path))
            self.assertEqual(method, "GET")
            if path.endswith("jobs/job"):
                return dict(id="job", createdAt="now", status="succeeded")
            self.assertTrue(path.startswith("/logs?"))
            return dict(
                hasMore=False,
                logs=[
                    dict(message=__import__("json").dumps(row))
                    for row in encode_result(outcome, "pin")
                ],
            )

        transport = RenderJob(dict(service_id="service"), request=request)
        self.assertEqual(transport.reconcile("job", "pin", "owner", Mock()), outcome)
        self.assertEqual(len(calls), 2)
        with self.assertRaisesRegex(ValueError, "wrong_pin"):
            transport.reconcile("job", "other", "owner", Mock())

    def test_reconcile_refuses_nonterminal_or_failed_job(self):
        for status in ("running", "failed", "canceled"):
            request = Mock(return_value=dict(id="job", createdAt="now", status=status))
            transport = RenderJob(dict(service_id="service"), request=request)
            with self.assertRaisesRegex(
                ValueError, "job_failed_requires_reconciliation"
            ):
                transport.reconcile("job", "pin", "owner", Mock())
            self.assertEqual(request.call_count, 1)

    def test_slow_preflight_cannot_reach_post(self):
        elapsed = [0]
        calls = []

        def request(method, path, body=None):
            calls.append(method)
            elapsed[0] += 45
            if method == "POST":
                raise AssertionError("expired preflight launched a job")
            if path.endswith("deploys?limit=1"):
                return [{"deploy": {"status": "live", "commit": {"id": "commit"}}}]
            if path.startswith("/services/service/jobs?"):
                return []
            return {"autoDeploy": "no", "ownerId": "owner"}

        transport = RenderJob(
            dict(service_id="service", commit="commit"),
            request=request,
            guard=lambda: {},
            clock=lambda: elapsed[0],
        )
        with self.assertRaisesRegex(ValueError, "preflight_deadline"):
            transport.run("pass", "pin", Mock())
        self.assertNotIn("POST", calls)

    def test_result_chunks_detect_missing_or_changed_output(self):
        result = {"rows": ["test" * 1000]}
        rows = encode_result(result, "pin")
        self.assertEqual(decode_result(rows, "pin"), result)
        with self.assertRaises(ValueError):
            decode_result(rows[:-1], "pin")
        with self.assertRaises(ValueError):
            decode_result(rows, "other")

    def test_post_unknown_records_intent_once(self):
        events = []

        def request(method, path, body=None):
            if method == "POST":
                self.assertEqual(events[-1][0], "launch_intent")
                raise TimeoutError()
            if path.endswith("deploys?limit=1"):
                return [{"deploy": {"status": "live", "commit": {"id": "commit"}}}]
            if path.startswith("/services/service/jobs?"):
                return []
            return {"autoDeploy": "no", "ownerId": "owner"}

        transport = RenderJob(
            dict(service_id="service", commit="commit"),
            request=request,
            guard=lambda: {"ok": True},
        )
        with self.assertRaises(TimeoutError):
            transport.run(
                "pass", "pin", lambda name, value: events.append((name, value))
            )
        self.assertEqual(sum(name == "launch_intent" for name, _ in events), 1)

    def test_missing_terminal_result_leaves_unknown_and_logs_only_after_terminal(self):
        terminal = False
        calls = []
        elapsed = [0]

        def request(method, path, body=None):
            nonlocal terminal
            calls.append((method, path))
            if method == "POST":
                return dict(id="job", createdAt="now", status="pending")
            if path.endswith("deploys?limit=1"):
                return [{"deploy": {"status": "live", "commit": {"id": "commit"}}}]
            if path.startswith("/services/service/jobs?"):
                return []
            if path.endswith("jobs/job"):
                terminal = True
                return dict(id="job", status="succeeded")
            if path.startswith("/logs?"):
                self.assertTrue(terminal)
                return dict(hasMore=False, logs=[])
            return {"autoDeploy": "no", "ownerId": "owner"}

        transport = RenderJob(
            dict(service_id="service", commit="commit"),
            request=request,
            guard=lambda: {},
            clock=lambda: elapsed[0],
            sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds),
        )
        with self.assertRaises(ValueError):
            transport.run("pass", "pin", Mock())
        self.assertEqual(sum(method == "POST" for method, _ in calls), 1)


class ActiveInventoryTests(unittest.TestCase):
    def transport(self, inventories):
        self.calls, self.events = [], []
        self.guard = Mock(return_value={})

        def request(method, path, body=None):
            self.calls.append((method, path))
            if method == "POST":
                self.assertEqual(self.events[-1][0], "launch_intent")
                return dict(id="job", createdAt="start", status="pending")
            if path.endswith("deploys?limit=1"):
                return [{"deploy": {"status": "live", "commit": {"id": "commit"}}}]
            if path.startswith("/services/service/jobs?"):
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
                self.assertEqual(set(query), {"limit", "status"})
                self.assertEqual(query["limit"], ["100"])
                self.assertIn(query["status"], [["pending"], ["running"]])
                return inventories[query["status"][0]]
            if path.endswith("jobs/job"):
                return dict(id="job", status="succeeded")
            if path.startswith("/logs?"):
                return dict(hasMore=False, logs=[
                    dict(message=json.dumps(row)) for row in encode_result({"ok": True}, "pin")
                ])
            return dict(autoDeploy="no", ownerId="owner")

        return RenderJob(dict(service_id="service", commit="commit"),
                         request=request, guard=self.guard)

    def test_empty_active_queries_admit_without_reading_terminal_history(self):
        transport = self.transport(dict(
            pending=[], running=[],
            succeeded=[dict(job=dict(id=f"done-{i}", status="succeeded")) for i in range(300)],
            failed=[dict(job=dict(id=f"failed-{i}", status="failed")) for i in range(300)],
        ))
        self.assertEqual(transport.run("pass", "pin", lambda *e: self.events.append(e)), {"ok": True})
        inventory = [path for method, path in self.calls if "jobs?" in path]
        self.assertEqual(len(inventory), 2)
        self.assertEqual([urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)["status"]
                          for path in inventory], [["pending"], ["running"]])
        self.assertEqual(sum(method == "POST" for method, _ in self.calls), 1)
        self.assertEqual(self.guard.call_count, 2)
        self.assertEqual(sum(path.endswith("deploys?limit=1") for _, path in self.calls), 2)

    def test_pending_or_running_row_with_cursor_blocks_before_intent(self):
        for status in ("pending", "running"):
            with self.subTest(status=status):
                values = dict(pending=[], running=[])
                values[status] = [dict(job=dict(id="busy", status=status), cursor="more")]
                transport = self.transport(values)
                with self.assertRaisesRegex(ValueError, "active_job"):
                    transport.run("pass", "pin", lambda *e: self.events.append(e))
                self.assertFalse(self.events)
                self.assertFalse(any(method == "POST" for method, _ in self.calls))
                self.assertFalse(any("cursor=" in path for _, path in self.calls))

    def test_ignored_filter_returning_terminal_history_fails_closed(self):
        for status in ("succeeded", "failed", "canceled"):
            with self.subTest(status=status):
                transport = self.transport(dict(pending=[dict(job=dict(id="old", status=status))], running=[]))
                with self.assertRaises(ValueError):
                    transport.run("pass", "pin", lambda *e: self.events.append(e))
                self.assertFalse(self.events)
                self.assertFalse(any(method == "POST" for method, _ in self.calls))

    def test_malformed_inventory_fails_closed(self):
        for invalid in ({}, {"jobs": []}, None, "", False, [None], [{}]):
            with self.subTest(invalid=invalid):
                transport = self.transport(dict(pending=invalid, running=[]))
                with self.assertRaises(ValueError):
                    transport.run("pass", "pin", lambda *e: self.events.append(e))
                self.assertFalse(self.events)
                self.assertFalse(any(method == "POST" for method, _ in self.calls))



class AvailabilityTests(unittest.TestCase):
    def transport(self, snapshots, *, status_seconds=0, log_seconds=0):
        self.now, self.calls, self.events, self.log_reads = 0, [], [], 0
        def request(method, path, body=None):
            self.calls.append((method, path))
            if method == "POST":
                return dict(id="job", createdAt="start", status="pending")
            if path.endswith("deploys?limit=1"):
                return [{"deploy": {"status": "live", "commit": {"id": "commit"}}}]
            if path.startswith("/services/service/jobs?"):
                return []
            if path.endswith("jobs/job"):
                self.now += status_seconds
                return dict(id="job", createdAt="start", status="succeeded")
            if path.startswith("/logs?"):
                self.now += log_seconds
                value = snapshots[min(self.log_reads, len(snapshots) - 1)]
                self.log_reads += 1
                if isinstance(value, Exception):
                    raise value
                if isinstance(value, dict):
                    return value
                return dict(hasMore=False, logs=[dict(message=json.dumps(row)) for row in value])
            return {"autoDeploy": "no", "ownerId": "owner"}
        def sleep(seconds):
            self.now += seconds
        return RenderJob(dict(service_id="service", commit="commit"), request=request,
                         guard=lambda: {}, clock=lambda: self.now, sleep=sleep)

    def test_delayed_complete_snapshot_uses_one_post_and_original_decoder(self):
        outcome = {"rows": list(range(2000))}
        frames = encode_result(outcome, "pin")
        self.assertGreater(len(frames), 2)
        transport = self.transport([[], frames[:1], frames[1:], frames])
        result = transport.run("pass", "pin", lambda *event: self.events.append(event))
        self.assertEqual(result, outcome)
        self.assertEqual(self.log_reads, 4)
        for method, path in self.calls:
            if path.startswith("/logs?"):
                query = urllib.parse.parse_qs(path.split("?", 1)[1])
                self.assertEqual(method, "GET")
                self.assertEqual(query["resource"], ["job"])
                self.assertEqual(query["ownerId"], ["owner"])
                self.assertEqual(query["startTime"], ["start"])
                self.assertEqual(query["limit"], ["1000"])
        self.assertEqual(sum(method == "POST" for method, _ in self.calls), 1)
        self.assertEqual(sum(name == "verified_job_result" for name, _ in self.events), 1)


    def test_incomplete_snapshots_are_not_combined_or_replayed(self):
        frames = encode_result({"rows": list(range(2000))}, "pin")
        transport = self.transport([frames[:-2] + [frames[-1]], frames[1:]])
        with self.assertRaisesRegex(ValueError, "remote_result_unavailable_before_deadline"):
            transport.run("pass", "pin", lambda *event: self.events.append(event))
        self.assertLessEqual(self.now, 360)
        self.assertGreater(self.log_reads, 1)
        self.assertEqual(sum(method == "POST" for method, _ in self.calls), 1)
        self.assertFalse(any(name == "verified_job_result" for name, _ in self.events))

    def test_invalid_snapshots_fail_once_even_if_a_good_snapshot_follows(self):
        frames = encode_result({"rows": list(range(2000))}, "pin")
        wrong_pin = [*frames[:-1], dict(frames[-1], pin="other")]
        wrong_digest = [*frames[:-1], dict(frames[-1], sha256="0" * 64)]
        cases = {
            "wrong pin": wrong_pin,
            "wrong digest": wrong_digest,
            "duplicate complete part": [frames[0], *frames],
            "duplicate partial part": [frames[0], frames[0]],
            "duplicate finish": [frames[-1], frames[-1]],
            "unordered partial parts": [frames[1], frames[0], frames[-1]],
            "malformed partial part": [dict(frames[0], data="invalid!")],
            "invalid part index": [dict(frames[0], part=-1)],
            "bool part index": [dict(frames[0], part=True)],
            "invalid finish count": [dict(frames[-1], parts=-1)],
            "has more": dict(hasMore=True, logs=[]),
            "malformed JSON": dict(hasMore=False, logs=[dict(message='{"diagnostic":"recall_retirement_job_v1",')]),
        }
        for name, snapshot in cases.items():
            with self.subTest(name=name):
                transport = self.transport([snapshot, frames])
                with self.assertRaises((ValueError, KeyError)):
                    transport.run("pass", "pin", lambda *event: self.events.append(event))
                self.assertEqual(self.log_reads, 1)
                self.assertEqual(self.now, 0)
                self.assertEqual(sum(method == "POST" for method, _ in self.calls), 1)
                self.assertFalse(any(event[0] == "verified_job_result" for event in self.events))

    def test_complete_corrupt_archive_fails_without_waiting_for_replacement(self):
        frames = encode_result([], "pin")
        frames[0] = dict(frames[0], data="bm90LWd6aXA=")
        transport = self.transport([frames, encode_result([], "pin")])
        with self.assertRaises(OSError):
            transport.run("pass", "pin", Mock())
        self.assertEqual(self.log_reads, 1)
        self.assertEqual(self.now, 0)

    def test_log_transport_error_is_not_an_availability_retry(self):
        transport = self.transport([TimeoutError("synthetic GET failure"), encode_result([], "pin")])
        with self.assertRaises(TimeoutError):
            transport.run("pass", "pin", Mock())
        self.assertEqual(self.log_reads, 1)
        self.assertEqual(sum(method == "POST" for method, _ in self.calls), 1)

    def test_remaining_deadline_reserves_the_configured_get_timeout(self):
        transport = self.transport([encode_result([], "pin")], status_seconds=321)
        with self.assertRaisesRegex(ValueError, "remote_result_unavailable_before_deadline"):
            transport.run("pass", "pin", Mock())
        self.assertEqual(self.log_reads, 0)
        self.assertEqual(self.now, 321)

    def test_late_get_response_is_not_verified(self):
        transport = self.transport([encode_result([], "pin")], status_seconds=319, log_seconds=41)
        with self.assertRaisesRegex(ValueError, "remote_result_unavailable_before_deadline"):
            transport.run("pass", "pin", lambda *event: self.events.append(event))
        self.assertEqual(self.log_reads, 1)
        self.assertFalse(any(name == "verified_job_result" for name, _ in self.events))

    def test_reconcile_remains_one_get_snapshot_without_post_or_poll(self):
        transport = self.transport([[], encode_result([], "pin")])
        with self.assertRaisesRegex(ValueError, "remote_result_missing_or_wrong_pin"):
            transport.reconcile("job", "pin", "owner", Mock())
        self.assertEqual(self.log_reads, 1)
        self.assertTrue(all(method == "GET" for method, _ in self.calls))
        self.assertEqual(self.now, 0)


class PaginationTests(unittest.TestCase):
    start = "2026-10-16T00:00:00+00:00"
    middle = "2026-10-16T00:00:01+00:00"

    def transport(self, pages, **kwargs):
        transport = AvailabilityTests.transport(self, pages, **kwargs)
        request = transport.request
        self.queries = []

        def page_request(method, path, body=None):
            value = request(method, path, body)
            if isinstance(value, dict) and "createdAt" in value:
                value = dict(value, createdAt=self.start)
            if path.startswith("/logs?"):
                query = dict(urllib.parse.parse_qsl(path.split("?", 1)[1]))
                self.queries.append(query)
                if isinstance(value, dict) and value.get("hasMore") is True:
                    value = dict(value)
                    value.setdefault("nextStartTime", self.middle)
                    value.setdefault("nextEndTime", query["endTime"])
            return value

        transport.request = page_request
        return transport

    def rows(self, pin="pin"):
        return [dict(id=str(n), timestamp=self.middle, message=json.dumps(frame))
                for n, frame in enumerate(encode_result({"ok": True}, pin))]

    def test_overlapping_pages_keep_original_job_and_single_post(self):
        rows = self.rows()
        transport = self.transport([dict(hasMore=True, logs=rows[:1]), dict(hasMore=False, logs=rows)])
        self.assertEqual(transport.run("pass", "pin", Mock()), {"ok": True})
        self.assertEqual(self.log_reads, 2)
        self.assertEqual(sum(method == "POST" for method, _ in self.calls), 1)
        self.assertEqual(self.queries[0]["startTime"], self.start)
        self.assertEqual(self.queries[1]["startTime"], self.middle)
        self.assertEqual(self.queries[0]["endTime"], self.queries[1]["endTime"])
        for query in self.queries:
            self.assertEqual({k: v for k, v in query.items() if k not in ("startTime", "endTime")},
                             dict(ownerId="owner", resource="job", type="app", direction="forward", limit="1000"))

    def test_reconcile_paginates_without_launch_or_poll(self):
        rows = self.rows()
        transport = self.transport([dict(hasMore=True, logs=rows[:1]), dict(hasMore=False, logs=rows[1:])])
        self.assertEqual(transport.reconcile("job", "pin", "owner", Mock()), {"ok": True})
        self.assertTrue(all(method == "GET" for method, _ in self.calls))
        self.assertEqual(self.log_reads, 2)
        self.assertEqual(self.now, 0)

    def test_narrowed_window_changes_only_provider_time_parameters(self):
        rows = self.rows()
        end = "2026-10-16T00:00:02+00:00"
        transport = self.transport([dict(hasMore=True, logs=rows[:1], nextEndTime=end),
                                    dict(hasMore=False, logs=rows[1:])])
        self.assertEqual(transport.reconcile("job", "pin", "owner", Mock()), {"ok": True})
        self.assertEqual(self.queries[1], dict(self.queries[0], startTime=self.middle, endTime=end))

    def test_conflicting_duplicate_identity_refuses(self):
        rows = self.rows()
        changed = dict(rows[0], message=rows[0]["message"] + " ")
        transport = self.transport([dict(hasMore=True, logs=rows[:1]), dict(hasMore=False, logs=[changed, rows[-1]])])
        with self.assertRaisesRegex(ValueError, "logs_duplicate_conflict"):
            transport.run("pass", "pin", lambda *event: self.events.append(event))
        self.assertFalse(any(name == "verified_job_result" for name, _ in self.events))

    def test_repeated_or_escaped_window_refuses(self):
        for window in (
            dict(nextStartTime=self.start), dict(nextStartTime="2026-10-15T00:00:00Z"),
            dict(nextEndTime="2099-01-01T00:00:00Z"), dict(nextStartTime="bad"),
            dict(nextStartTime="2026-10-16T00:00:01"),
        ):
            with self.subTest(window=window):
                transport = self.transport([dict(hasMore=True, logs=[], **window)])
                with self.assertRaisesRegex(ValueError, "logs_window"):
                    transport.reconcile("job", "pin", "owner", Mock())
                self.assertEqual(self.log_reads, 1)
        transport = self.transport([dict(hasMore=True, logs=[])])
        with self.assertRaisesRegex(ValueError, "logs_window"):
            transport.reconcile("job", "pin", "owner", Mock())
        self.assertEqual(self.log_reads, 2)

    def test_paginated_partial_snapshot_is_not_merged_with_later_snapshot(self):
        rows = self.rows()
        transport = self.transport([dict(hasMore=True, logs=rows[:1]), dict(hasMore=False, logs=[]),
                                    dict(hasMore=False, logs=rows[1:])])
        with self.assertRaisesRegex(ValueError, "remote_result_unavailable_before_deadline"):
            transport.run("pass", "pin", Mock())
        self.assertEqual(sum(method == "POST" for method, _ in self.calls), 1)
        self.assertGreater(self.log_reads, 2)

    def test_each_page_reserves_deadline_and_refuses_late_response(self):
        for status_seconds, log_seconds, reads in ((290, 31, 1), (270, 45, 2)):
            with self.subTest(status_seconds=status_seconds):
                rows = self.rows()
                transport = self.transport([dict(hasMore=True, logs=rows[:1]), dict(hasMore=False, logs=rows[1:])],
                                           status_seconds=status_seconds, log_seconds=log_seconds)
                with self.assertRaisesRegex(ValueError, "remote_result_unavailable_before_deadline"):
                    transport.run("pass", "pin", Mock())
                self.assertEqual(self.log_reads, reads)
        transport = self.transport([dict(hasMore=True, logs=[])], log_seconds=51)
        with self.assertRaisesRegex(ValueError, "remote_result_unavailable_before_deadline"):
            transport.reconcile("job", "pin", "owner", Mock())
        self.assertEqual(self.log_reads, 1)

    def test_missing_identity_does_not_hide_duplicate_frame(self):
        rows = [dict(message=row["message"]) for row in self.rows()]
        transport = self.transport([dict(hasMore=True, logs=rows[:1]), dict(hasMore=False, logs=rows)])
        with self.assertRaisesRegex(ValueError, "remote_result_parts"):
            transport.reconcile("job", "pin", "owner", Mock())

    def test_wrong_pin_after_pagination_stays_refused(self):
        rows = self.rows(pin="wrong")
        transport = self.transport([dict(hasMore=True, logs=rows[:1]), dict(hasMore=False, logs=rows[1:])])
        with self.assertRaisesRegex(ValueError, "wrong_pin"):
            transport.reconcile("job", "pin", "owner", Mock())

    def test_bounds_and_malformed_pages_refuse(self):
        from recall_server import retirement_jobs as jobs
        for field, bound, pages in (
            ("LOG_MAX_ROWS", 1, [dict(hasMore=False, logs=self.rows())]),
            ("LOG_MAX_BYTES", 1, [dict(hasMore=False, logs=[dict(message="ordinary log")])]),
            ("LOG_MAX_PAGES", 1, [dict(hasMore=True, logs=[])]),
        ):
            with self.subTest(field=field), patch.object(jobs, field, bound):
                transport = self.transport(pages)
                with self.assertRaisesRegex(ValueError, "logs_bound"):
                    transport.reconcile("job", "pin", "owner", Mock())
                self.assertEqual(self.log_reads, 1)
        for page in (dict(hasMore=False), dict(hasMore=False, logs=None), dict(hasMore=False, logs=[None]),
                     dict(hasMore=False, logs=[dict(message=42)]), dict(hasMore=0, logs=[])):
            with self.subTest(page=page):
                transport = self.transport([page])
                with self.assertRaisesRegex(ValueError, "logs_shape"):
                    transport.reconcile("job", "pin", "owner", Mock())


if __name__ == "__main__":
    unittest.main()
