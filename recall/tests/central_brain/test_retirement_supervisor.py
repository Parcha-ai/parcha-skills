"""A slow proof cannot block projection or create overlapping retirement children."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from recall_server.retirement_supervisor import RetirementSupervisor


class RetirementSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.children = []
        # unittest discovery adds these roots to sys.path, not the environment
        # inherited by a real child process. Bind the same checkout explicitly.
        recall_root = Path(__file__).resolve().parents[2]
        self.child_environment = dict(os.environ, PYTHONPATH=os.pathsep.join((
            str(recall_root), str(recall_root / "server"),
            os.environ.get("PYTHONPATH", ""))))

        def reap():
            for child in self.children:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=2)
                if child.stdout is not None:
                    child.stdout.close()

        self.addCleanup(reap)

        def launch(_configuration):
            command = """import json,sys,time
import os
from pathlib import Path
root=Path(sys.argv[1]);(root/'ready').touch()
while not (root/'release').exists():time.sleep(.005)
print(json.dumps({'status':'complete','cleared_documents':3,'cleared_utf8_bytes':99}))
"""
            child = subprocess.Popen(
                [sys.executable, "-c", command, str(self.root)],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            self.children.append(child)
            return child

        self.worker = RetirementSupervisor(
            "postgresql://synthetic.invalid/brain",
            tenant_id="tenant:test",
            seconds=180,
            launch=launch,
        )
        self.addCleanup(self.worker.close)

    def wait_until(self, condition):
        end = time.monotonic() + 3
        while not condition():
            if time.monotonic() > end:
                self.fail("bounded child condition timed out")
            time.sleep(0.005)

    def test_foreground_progress_and_one_child_while_proof_is_blocked(self):
        first = self.worker.cycle(busy=True)
        self.assertEqual(first["status"], "running")
        self.wait_until(lambda: (self.root / "ready").exists())
        started = time.monotonic()
        foreground = []
        for i in range(50):
            foreground.append(i)
            self.assertEqual(self.worker.cycle(busy=True)["status"], "running")
        self.assertEqual(len(foreground), 50)
        self.assertLess(time.monotonic() - started, 0.2)
        self.assertEqual(len(self.children), 1)

    def test_completed_counters_are_delivered_once(self):
        self.worker.cycle(busy=True)
        (self.root / "release").touch()
        self.wait_until(lambda: self.children[0].poll() is not None)
        report = self.worker.cycle(busy=True)
        self.assertEqual(report["cleared_documents"], 3)
        self.assertEqual(report["cleared_utf8_bytes"], 99)
        self.assertEqual(self.worker.cycle(busy=False)["cleared_documents"], 0)

    def test_phase_timers_reach_existing_content_free_completion_log(self):
        from io import BytesIO
        from unittest.mock import Mock
        result = dict(status='commit_outcome_unknown', cleared_documents=1,
            cleared_utf8_bytes=99, commit_outcome_unknown=True,
            committed_sql_ms=15., committed_hash_ms=3., archive_read_ms=8.,
            archive_reused_gets=2, archive_reused_bytes=1234,
            claim_wall_ms=2., parent_wall_ms=31., metadata_capture_ms=5., error_class="LogicalEvidenceError",
            cause_error_code="logical_evidence_record_invalid", phase="maintenance",
            metadata_rows=3, metadata_fetches=2, metadata_fetch_ms=2.,
            metadata_membership_calls=1, metadata_membership_ms=1., metadata_spool_ms=.5,
            attempted_parents=2, failed_parents=2,
            errors={"parent_retirement_deadline_exceeded": 2})
        self.worker.child = Mock(returncode=0, stdout=BytesIO(json.dumps(result).encode()))
        self.worker.child.poll.return_value = 0
        with self.assertLogs('recall_server.retirement_supervisor', level='INFO') as logs:
            observed = self.worker.cycle()
        self.assertTrue(observed['commit_outcome_unknown'])
        for field in ('committed_sql_ms','committed_hash_ms','archive_read_ms',
                      'archive_reused_gets','archive_reused_bytes',
                      'claim_wall_ms','parent_wall_ms','metadata_capture_ms','cause_error_code',
                      'metadata_rows','metadata_fetches','metadata_fetch_ms',
                      'metadata_membership_calls','metadata_membership_ms','metadata_spool_ms',
                      'attempted_parents','failed_parents','errors'):
            self.assertIn(f'{field}={result[field]}', logs.output[0])

    def test_scheduling_completion_is_logged_separately_from_archive_proofs(self):
        from io import BytesIO
        from unittest.mock import Mock
        for counters in ({'completed_noop_parents': 2, 'completed_parent_proofs': 1}, {}):
            with self.subTest(counters=counters):
                result = dict(status='complete', cleared_documents=0,
                    cleared_utf8_bytes=0, **counters)
                self.worker.child = Mock(returncode=0, stdout=BytesIO(json.dumps(result).encode()))
                self.worker.child.poll.return_value = 0
                with self.assertLogs('recall_server.retirement_supervisor', level='INFO') as logs:
                    observed = self.worker.cycle()
                self.assertEqual(observed, result)
                for field in ('completed_noop_parents', 'completed_parent_proofs'):
                    self.assertIn(f'{field}={counters.get(field)}', logs.output[0])

    def test_unknown_claim_identity_reaches_existing_log(self):
        from io import BytesIO
        from unittest.mock import Mock
        result = dict(status='commit_outcome_unknown', cleared_documents=0,
            cleared_utf8_bytes=0, commit_outcome_unknown=True,
            unknown_source_id='source:claimed', unknown_native_parent_id='parent\nclaimed',
            unknown_scope_epoch=17)
        self.worker.child = Mock(returncode=0, stdout=BytesIO(json.dumps(result).encode()))
        self.worker.child.poll.return_value = 0
        with self.assertLogs('recall_server.retirement_supervisor', level='INFO') as logs:
            observed = self.worker.cycle()
        self.assertEqual(observed, result)
        self.assertIn("unknown_source_id='source:claimed'", logs.output[0])
        self.assertIn("unknown_native_parent_id=" + repr('parent\nclaimed'), logs.output[0])
        self.assertIn('unknown_scope_epoch=17', logs.output[0])
        self.assertNotIn('maintenance_epoch', observed)
        self.assertNotIn('intent_acknowledged', observed)

    def test_shutdown_reaps_exact_child_and_refuses_another_launch(self):
        self.worker.cycle(busy=True)
        self.wait_until(lambda: (self.root / "ready").exists())
        self.worker.close()
        self.assertIsNotNone(self.children[0].poll())
        self.assertEqual(self.worker.cycle(busy=False)["status"], "closed")
        self.assertEqual(len(self.children), 1)

    def test_child_loss_preserves_unknown_outcome_instead_of_success(self):
        self.worker.cycle(busy=False)
        self.children[0].kill()
        self.children[0].wait(timeout=2)
        report = self.worker.cycle(busy=False)
        self.assertEqual(report["status"], "commit_outcome_unknown")
        self.assertTrue(report["commit_outcome_unknown"])
        self.assertEqual(report["cleared_documents"], 0)
        for field in ("unknown_source_id", "unknown_native_parent_id", "unknown_scope_epoch"):
            self.assertNotIn(field, report)

    def test_parent_sigterm_reaps_child_and_restores_handler(self):
        before = signal.getsignal(signal.SIGTERM)
        with self.assertRaises(SystemExit):
            with self.worker:
                self.worker.cycle(busy=True)
                os.kill(os.getpid(), signal.SIGTERM)
        self.assertIsNotNone(self.children[0].poll())
        self.assertEqual(signal.getsignal(signal.SIGTERM), before)
        self.assertTrue(self.worker.closed)

    def test_invalid_result_preserves_unknown_outcome(self):
        def launch(_configuration):
            return subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    'print(\'{"status":"complete","cleared_documents":true}\')',
                ],
                stdout=subprocess.PIPE,
            )

        self.worker.launch = launch
        self.worker.cycle(busy=False)
        self.wait_until(lambda: self.worker.child.poll() is not None)
        report = self.worker.cycle(busy=False)
        self.assertTrue(report["commit_outcome_unknown"])
        self.assertEqual(report["cleared_documents"], 0)

    def test_child_deadline_survives_parent_sigkill(self):
        child_script = """import os,time
from recall_server import retirement_process as p
class Store:
    def __init__(self,dsn):pass
    def close(self):pass
class Owner:
    def __init__(self,*args,**kwargs):pass
    def cycle(self,**kwargs):
        open(os.environ['RETIREMENT_TEST_READY'],'w').close()
        time.sleep(60)
p.RetirementStore=Store
p.RetirementMaintenance=Owner
p.build_evidence_archive_store=lambda **kwargs:object()
p.main()
"""
        parent_script = """import subprocess,sys,json,time
child=subprocess.Popen([sys.executable,'-c',sys.argv[1]],stdin=subprocess.PIPE,
    stderr=subprocess.DEVNULL)
child.stdin.write(json.dumps(dict(dsn='synthetic',tenant_id=None,seconds=.1)).encode())
child.stdin.close()
print(child.pid,flush=True)
time.sleep(60)
"""
        parent = subprocess.Popen(
            [sys.executable, "-c", parent_script, child_script],
            stdout=subprocess.PIPE,
            env=dict(
                self.child_environment,
                RECALL_CHUNK_BODY_READS="archive",
                RETIREMENT_TEST_READY=str(self.root / "child-ready"),
            ),
        )
        child_pid = int(parent.stdout.readline())
        try:
            self.wait_until(lambda: (self.root / "child-ready").exists())
            parent.kill()
            # The child inherited stdout. EOF requires its own alarm to end it,
            # even though the supervisor process can no longer send a signal.
            parent.communicate(timeout=8)
        finally:
            if parent.poll() is None:
                parent.kill()
                parent.wait(timeout=2)
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            parent.stdout.close()

    def test_child_failure_emits_content_free_phase_and_error_class(self):
        code = """from recall_server import retirement_process as p
class Store:
    def __init__(self,dsn):pass
    def close(self):pass
def unavailable(**kwargs):raise RuntimeError('sensitive-provider-and-dsn-value')
p.RetirementStore=Store
p.build_evidence_archive_store=unavailable
p.main()
"""
        child = subprocess.run(
            [sys.executable, "-c", code],
            input=json.dumps(
                dict(dsn="sensitive-provider-and-dsn-value", tenant_id=None, seconds=1)
            ),
            text=True,
            capture_output=True,
            env=dict(self.child_environment, RECALL_CHUNK_BODY_READS="archive"),
            timeout=5,
        )
        self.assertEqual(child.returncode, 1)
        self.assertTrue(child.stdout, child.stderr)
        report = json.loads(child.stdout)
        self.assertEqual(report["error_class"], "RuntimeError")
        self.assertEqual(report["phase"], "archive_setup")
        self.assertTrue(report["commit_outcome_unknown"])
        self.assertEqual(report["cleared_documents"], 0)
        self.assertNotIn(
            "sensitive-provider-and-dsn-value", child.stdout + child.stderr
        )

    def test_nonzero_child_preserves_safe_failure_envelope(self):
        def launch(_configuration):
            return subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    "import json;print(json.dumps(dict(status='commit_outcome_unknown', "
                    "commit_outcome_unknown=True,cleared_documents=0,cleared_utf8_bytes=0,"
                    "error_class='RuntimeError',phase='archive_setup')));raise SystemExit(1)",
                ],
                stdout=subprocess.PIPE,
            )

        self.worker.launch = launch
        self.worker.cycle(busy=False)
        self.wait_until(lambda: self.worker.child.poll() is not None)
        report = self.worker.cycle(busy=False)
        self.assertEqual(report["error_class"], "RuntimeError")
        self.assertEqual(report["phase"], "archive_setup")
        self.assertTrue(report["commit_outcome_unknown"])

    def test_configuration_failure_reaps_owned_child(self):
        child = subprocess.Popen(
            [sys.executable, "-c", "import time;time.sleep(60)"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        self.children.append(child)
        self.worker.launch = lambda _configuration: child
        with patch.object(child.stdin, "write", side_effect=BrokenPipeError):
            with self.assertRaises(BrokenPipeError):
                self.worker.cycle(busy=True)
        self.assertIsNotNone(child.poll())
        self.assertIsNone(self.worker.child)
        child.stdin.close()

    def test_failed_reap_keeps_child_owned_and_cannot_spawn_overlap(self):
        self.worker.cycle(busy=True)
        child = self.children[0]
        self.worker.started_at = time.monotonic() - 200
        with (
            patch.object(child, "terminate"),
            patch.object(child, "kill"),
            patch.object(
                child, "wait", side_effect=subprocess.TimeoutExpired("child", 2)
            ),
        ):
            with self.assertRaises(subprocess.TimeoutExpired):
                self.worker.cycle(busy=True)
        self.assertIs(self.worker.child, child)
        self.assertEqual(len(self.children), 1)

    def test_deadline_terminates_one_stuck_child(self):
        self.worker.cycle(busy=True)
        self.worker.started_at = time.monotonic() - 200
        report = self.worker.cycle(busy=True)
        self.assertEqual(report["status"], "commit_outcome_unknown")
        self.assertIsNotNone(self.children[0].poll())
        self.assertEqual(len(self.children), 1)


if __name__ == "__main__":
    unittest.main()
