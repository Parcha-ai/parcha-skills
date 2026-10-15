"""Offline, scripted colleagues using Tether's real queue, driver and reply path.

The computers and Slack host are in-memory simulations. The artifact and its
independent review oracle are real; no model, native process or network runs.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import importlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import ModuleType, SimpleNamespace
import uuid


INITIAL = """def median(values):
    values = sorted(values)
    if not values:
        raise ValueError('median requires at least one value')
    return values[len(values) // 2]
"""
CORRECTED = """def median(values):
    values = sorted(values)
    if not values:
        raise ValueError('median requires at least one value')
    middle = len(values) // 2
    if len(values) % 2:
        return values[middle]
    return (values[middle - 1] + values[middle]) / 2
"""
CASES = (([7], 7), ([9, 1, 5], 5), ([1, 5], 3), ([-4, -2], -3))


class DemoFailure(RuntimeError):
    """The offline example did not demonstrate its claimed outcome."""


def forbidden(*_args, **_kwargs):
    raise DemoFailure("Live factory, process, discovery or transport is forbidden in demo")


@contextmanager
def _core():
    # A private namespace avoids modifying any already imported gateway classes.
    name = "_tether_offline_demo_" + uuid.uuid4().hex
    package = ModuleType(name)
    package.__path__ = [str(Path(__file__).resolve().parent)]
    sys.modules[name] = package
    previous = os.environ.get("TETHER_HERDR")
    os.environ["TETHER_HERDR"] = "off"
    try:
        modules = {key: importlib.import_module(name + "." + key)
                   for key in ("admission", "active", "store", "session_driver", "herdr")}
        modules["herdr"].Herdr.discover = classmethod(forbidden)
        modules["herdr"].Herdr._run = forbidden
        yield SimpleNamespace(**modules)
    finally:
        if previous is None:
            os.environ.pop("TETHER_HERDR", None)
        else:
            os.environ["TETHER_HERDR"] = previous
        for key in list(sys.modules):
            if key == name or key.startswith(name + "."):
                sys.modules.pop(key, None)


def _function(path):
    # Only the demo's own fixed sample is executable, never arbitrary file input.
    source = path.read_text(encoding="utf-8")
    if source not in (INITIAL, CORRECTED):
        raise DemoFailure("Sample artifact differs from the demo's permitted programs")
    scope = {"__builtins__": {"sorted": sorted, "len": len, "ValueError": ValueError}}
    # Only the two exact bundled sample programs pass the allowlist above.
    exec(compile(source, "offline-demo/median.py", "exec"), scope)  # nosec B102
    return scope["median"]


def artifact_oracle(path):
    function = _function(path)
    observations = []
    for values, expected in CASES:
        actual = function(list(values))
        observations.append({"input": values, "expected": expected,
                             "actual": actual, "passed": actual == expected})
    try:
        function([])
    except ValueError:
        empty_ok = True
    else:
        empty_ok = False
    observations.append({"input": [], "expected_exception": "ValueError", "passed": empty_ok})
    return {"passed": all(row["passed"] for row in observations), "cases": observations,
            "artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


class _Computer:
    """A scripted in-memory SessionProcess seam, not an AI or native process."""

    def __init__(self, role, session_id, artifact):
        self.role, self.session_id, self.artifact = role, session_id, artifact
        self.lock = threading.Lock()
        self.last_used = time.monotonic()
        self.calls = []
        self.reviews = []
        self.results = []
        self.pending = None

    def send(self, prompt):
        self.calls.append({"session_id": self.session_id,
                           "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest()})
        if self.role == "implementer":
            if len(self.calls) == 1:
                if "Implement median(values)" not in prompt:
                    raise DemoFailure("Implementer did not receive the original task")
                self.artifact.write_text(INITIAL, encoding="utf-8")
                self.pending = "Implemented median.py. Review the actual artifact before accepting the task."
            elif len(self.calls) == 2:
                if "expected 3" not in prompt or "actual 5" not in prompt:
                    raise DemoFailure("The actual review counterexample did not reach the implementer")
                self.artifact.write_text(CORRECTED, encoding="utf-8")
                checked = artifact_oracle(self.artifact)
                if not checked["passed"]:
                    raise DemoFailure("Correction did not fix the actual artifact")
                self.pending = "Corrected median.py; all five local oracle cases pass. Please independently re-review."
            elif len(self.calls) == 3:
                if "Follow-up" not in prompt:
                    raise DemoFailure("Original session did not receive the follow-up")
                answer = _function(self.artifact)([2, 4])
                self.pending = f"The corrected artifact computes median([2, 4]) = {answer:g}."
            else:
                raise DemoFailure("Unexpected extra implementer turn")
        else:
            if "Review" not in prompt:
                raise DemoFailure("Reviewer did not receive artifact review work")
            result = artifact_oracle(self.artifact)
            self.reviews.append(result)
            if result["passed"]:
                self.pending = "Independent review passed: all five actual artifact cases pass. The task is complete."
            else:
                failure = next(row for row in result["cases"] if not row["passed"])
                self.pending = (f"Review requires changes: median({failure['input']}) "
                                f"expected {failure['expected']}, actual {failure['actual']}. "
                                "Average the middle pair for even-length inputs.")

    def read_result(self, _timeout):
        return {"type": "result", "session_id": self.session_id,
                "result": self.pending, "is_error": False}

    def alive(self):
        return True

    def terminate(self):
        pass


def _names(team):
    if team is None:
        return ("Engineer", "Reviewer")
    colleagues = list(team.colleagues)
    if len(colleagues) < 2:
        raise DemoFailure("Demo team needs at least two colleagues")
    implementer = next((c for c in colleagues if c.id == team.self_id), colleagues[0])
    reviewer = next(c for c in colleagues if c.id != implementer.id)
    return implementer.name, reviewer.name


def run_demo(*, team=None):
    """Return a CI receipt, cleaning every sample/state file before returning."""
    names = _names(team)
    receipt = {"schema": "tether-offline-demo/v1", "simulated": True,
               "scope": "Scripted fake computers and fake Slack host; real Tether core and tested sample artifact",
               "actual_model_calls": 0, "actual_slack_calls": 0, "native_processes": 0,
               "colleagues": {"implementer": names[0], "reviewer": names[1]},
               "turns": [], "deliveries": [], "checks": {}}
    with tempfile.TemporaryDirectory(prefix="tether-offline-demo-") as directory, _core() as core:
        root = Path(directory)
        artifact = root / "median.py"
        stores, drivers, colleagues, computers = {}, {}, {}, {}
        actors = {"implementer": "UDEMOENGINEER", "reviewer": "UDEMOREVIEWER"}
        try:
            for role in actors:
                base = root / role
                base.mkdir(mode=0o700)
                store = core.store.Store(base / "tether.db")
                stores[role] = store
                settings = core.active.ActiveSettings(enabled=True, launcher="direct", presence=False)
                # The real shared team prompt sees only explicitly simulated identities.
                if hasattr(settings, "team"):
                    manifest = core.active.team_module
                    settings = replace(settings, team=manifest.TeamManifest(self_id=role, colleagues=tuple(
                        manifest.Colleague(id=key, name=names[index],
                            role="Sample implementation" if index == 0 else "Independent artifact review",
                            slack_id=actors[key])
                        for index, key in enumerate(actors))))
                computer = _Computer(role, "simulated-native-" + role, artifact)
                computers[role] = computer
                read_result = computer.read_result
                def observed_result(timeout, computer=computer, read_result=read_result):
                    result = read_result(timeout)
                    computer.results.append({"session_id": result.get("session_id"),
                        "text_sha256": hashlib.sha256(str(result.get("result") or "").encode()).hexdigest()})
                    return result
                computer.read_result = observed_result
                driver = core.session_driver.SessionDriver(store, base / "session", settings,
                    launch_plan=forbidden, child_env=forbidden, popen=forbidden)
                drivers[role] = driver
                driver._session_for = lambda *_a, computer=computer, **_k: computer
                driver.herdr_factory = forbidden
                def deliver(channel, thread, text, role=role):
                    ack = f"1000000000.{len(receipt['deliveries']) + 1:06d}"
                    receipt["deliveries"].append({"profile": role, "channel": channel,
                        "thread": thread, "text": text, "message_id": ack,
                        "simulated_ack": True, "text_sha256": hashlib.sha256(text.encode()).hexdigest()})
                descriptor = SimpleNamespace(authorized_owner_ids=("UDEMOHUMAN",),
                    canonical_owner_ids=("UDEMOHUMAN",), workspace_id="TDEMO",
                    persona_id=role, policy_generation=1)
                colleague = core.active.ActiveSlice(runtime=store, driver=driver, settings=settings,
                    egress=deliver, descriptor=descriptor)
                colleague.herdr_factory = forbidden
                colleague._create_session = forbidden
                colleague.bind(source_kind="claude_session", session_id=computer.session_id,
                    cwd=str(root), team_id="TDEMO", channel_id="CDEMO", thread_ts="100.1",
                    owner_user_id="UDEMOHUMAN", spawned=True)
                colleagues[role] = colleague

            def turn(role, actor, text, phase):
                fields = {"platform": "slack", "workspace": "TDEMO", "channel": "CDEMO",
                    "thread": "100.1", "actor": actor, "actor_is_bot": actor != "UDEMOHUMAN",
                    "message_id": f"200.{len(receipt['turns']) + 1:06d}"}
                policy = core.admission.AdmissionSettings("TDEMO", frozenset({"UDEMOHUMAN"}),
                    frozenset(actors.values()), actors[role])
                decision = core.admission.evaluate(**fields, text=text, settings=policy,
                                                  bound_threads={("CDEMO", "100.1")})
                if decision["verdict"] != "admit":
                    raise DemoFailure("Demo message failed actual admission")
                claimed = colleagues[role].claim(fields, text, peer=fields["actor_is_bot"],
                    peers=frozenset(actors.values()), self_user_id=actors[role])
                before = len(receipt["deliveries"])
                if not claimed or colleagues[role].run_once() != 1:
                    raise DemoFailure("Core did not execute the admitted turn")
                rows = list(stores[role]._db.execute("SELECT * FROM attempts ORDER BY rowid"))
                attempt = dict(rows[-1])
                source = stores[role].attempt_context(attempt["attempt_id"])["source"]
                if attempt["state"] != "completed_with_response" or len(receipt["deliveries"]) != before + 1:
                    raise DemoFailure("Core did not save and deliver a completed response")
                delivery = receipt["deliveries"][-1]
                saved = Path(attempt["response_ref"]).read_text(encoding="utf-8")
                frame = computers[role].results[-1]
                if frame["session_id"] != source["session_id"]:
                    raise DemoFailure("Simulated native result did not belong to the original session")
                if (saved != delivery["text"] or delivery["profile"] != role
                        or frame["text_sha256"] != hashlib.sha256(saved.encode()).hexdigest()
                        or delivery["channel"] != "CDEMO" or delivery["thread"] != "100.1"):
                    raise DemoFailure("Saved answer and fake-host delivery do not match")
                accepted = bool(computers["reviewer"].reviews and computers["reviewer"].reviews[-1]["passed"])
                receipt["turns"].append({"phase": phase, "profile": role,
                    "attempt_id": attempt["attempt_id"], "event_key": claimed["event_key"],
                    "binding_id": attempt["binding_id"], "endpoint_id": attempt["endpoint_id"],
                    "native_session_id": source["session_id"], "state": attempt["state"],
                    "actor": actor, "actor_is_simulated_human": actor == "UDEMOHUMAN",
                    "native_result_session_id": frame["session_id"],
                    "saved_answer_sha256": hashlib.sha256(saved.encode()).hexdigest(),
                    "fake_host_message_id": delivery["message_id"],
                    "artifact_accepted": accepted})
                return delivery["text"]

            first = turn("implementer", "UDEMOHUMAN",
                "Implement median(values) in median.py. Review even-length and empty inputs before accepting completion.", "implementation")
            initial_source = artifact.read_text(encoding="utf-8")
            feedback = turn("reviewer", actors["implementer"], "Review the actual median.py artifact. " + first, "review_fail")
            correction = turn("implementer", actors["reviewer"], feedback, "correction")
            turn("reviewer", actors["implementer"], "Review the corrected median.py artifact. " + correction, "review_pass")
            followup = turn("implementer", "UDEMOHUMAN", "Follow-up: use your corrected artifact to compute median([2, 4]).", "followup")
            receipt["artifact"] = {"name": "median.py", "initial_source": initial_source,
                "final_source": artifact.read_text(encoding="utf-8"),
                "initial_review": computers["reviewer"].reviews[0],
                "final_review": computers["reviewer"].reviews[1]}
            receipt["task_outcome"] = {"state": "completed" if computers["reviewer"].reviews[1]["passed"] else "needs_changes",
                "accepted_by": "independent artifact oracle", "accepted_at_phase": "review_pass",
                "completed_tasks": int(computers["reviewer"].reviews[1]["passed"]),
                "completed_transport_attempts": len(receipt["turns"])}
            receipt["computer_turn_counts"] = {role: len(computer.calls) for role, computer in computers.items()}
            receipt["core_source_sha256"] = {key: hashlib.sha256(Path(getattr(core, key).__file__).read_bytes()).hexdigest()
                for key in ("admission", "active", "store", "session_driver")}
            receipt["checks"] = {
                "initial_artifact_really_fails": not receipt["artifact"]["initial_review"]["passed"],
                "corrected_artifact_really_passes": receipt["artifact"]["final_review"]["passed"],
                "artifact_changed": receipt["artifact"]["initial_review"]["artifact_sha256"] != receipt["artifact"]["final_review"]["artifact_sha256"],
                "same_implementer_session": len({t["native_session_id"] for t in receipt["turns"] if t["profile"] == "implementer"}) == 1,
                "same_reviewer_session": len({t["native_session_id"] for t in receipt["turns"] if t["profile"] == "reviewer"}) == 1,
                "every_result_owned_by_selected_session": all(t["native_result_session_id"] == t["native_session_id"] for t in receipt["turns"]),
                "human_followup_preserves_owner": receipt["turns"][-1]["actor_is_simulated_human"] and receipt["turns"][0]["binding_id"] == receipt["turns"][-1]["binding_id"],
                "followup_uses_corrected_artifact": followup.endswith("= 3."),
                "five_distinct_core_attempts": len({t["attempt_id"] for t in receipt["turns"]}) == 5,
                "five_fake_host_deliveries": len(receipt["deliveries"]) == 5,
                "no_extra_simulated_computation": receipt["computer_turn_counts"] == {"implementer": 3, "reviewer": 2},
                "queues_empty": all(colleague.run_once() == 0 for colleague in colleagues.values()),
                "no_promise_counted_as_completion": not any(t["artifact_accepted"] for t in receipt["turns"][:3]),
            }
        finally:
            for driver in drivers.values():
                driver.shutdown()
            for store in stores.values():
                store.close()
    receipt["checks"]["temporary_files_removed"] = not root.exists()
    receipt["ok"] = all(receipt["checks"].values())
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print the simulated CI evidence receipt")
    parser.add_argument("--team-config", type=Path, help="Use names from an explicit team manifest")
    args = parser.parse_args(argv)
    try:
        team = None
        if args.team_config:
            with _core():
                # Team manifests supply names only; real Slack/computer settings are not used.
                spec = importlib.util.spec_from_file_location("_tether_demo_team", Path(__file__).with_name("team.py"))
                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module
                try:
                    spec.loader.exec_module(module)
                    team = module.load_manifest(args.team_config)
                finally:
                    sys.modules.pop(spec.name, None)
        result = run_demo(team=team)
    except Exception as exc:
        result = {"schema": "tether-offline-demo/v1", "simulated": True, "ok": False,
                  "error": str(exc), "error_type": type(exc).__name__}
    if args.json:
        print(json.dumps(result, sort_keys=True))
    elif result["ok"]:
        engineer, reviewer = result["colleagues"]["implementer"], result["colleagues"]["reviewer"]
        print("Offline simulation — no Slack, model or native process was used.\n"
              f"1. {engineer} created median.py.\n"
              f"2. {reviewer} tested it: [1, 5] returned 5; expected 3.\n"
              f"3. Original {engineer} session corrected the artifact.\n"
              "4. Independent review: all five cases pass.\n"
              "5. Same-session follow-up: median([2, 4]) = 3.\n"
              "PASS: five core attempts and fake-host deliveries; temporary files cleaned.")
    else:
        print("Offline demo failed: " + result.get("error", "Evidence checks failed"), file=sys.stderr)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
