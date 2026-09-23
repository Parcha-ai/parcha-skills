"""Untrusted commands start only after the capability boundary succeeds."""
import hashlib
import shlex
import subprocess
import unittest

from recall_server.deep_inspection import AgentExecObject, _agent_exec_command


class CapabilityBoundaryTest(unittest.TestCase):
    def tail(self, scan):
        digest = hashlib.sha256(b"synthetic").hexdigest()
        obj = AgentExecObject(f"objects/{digest[:2]}/{digest}", digest)
        command = _agent_exec_command(
            program="printf USER_PROGRAM_EXECUTED", objects=(obj,),
            document_aliases={}, record_spans={}, routing_receipts={},
            timeout_seconds=10,
            dataset_aliases={obj.object_key: "s1/2026-09/documents-part-00000.parquet"} if scan else None,
        )
        inner = shlex.split(command.rsplit("\nunshare ", 1)[-1])[-1]
        tokens = shlex.split(inner)
        return shlex.join(tokens[max(i for i, token in enumerate(tokens) if token == "exec"):])

    def test_exec_and_scan_drop_all_capability_sets_before_user_shell(self):
        for scan in (False, True):
            with self.subTest(scan=scan):
                args = shlex.split(self.tail(scan))
                self.assertEqual(args[:7], [
                    "exec", "setpriv", "--bounding-set=-all", "--inh-caps=-all",
                    "--ambient-caps=-all", "--no-new-privs", "env",
                ])
                self.assertIn("-i", args)
                self.assertIn("timeout --signal=KILL 10s", args[-1])

    def test_missing_security_utility_cannot_fall_through_to_program(self):
        result = subprocess.run(
            ["/bin/bash", "-c", self.tail(True)],
            env={"PATH": "/nonexistent"}, capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 127)
        self.assertNotIn("USER_PROGRAM_EXECUTED", result.stdout)


if __name__ == "__main__":
    unittest.main()
