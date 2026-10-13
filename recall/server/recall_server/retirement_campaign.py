"""Durable scheduling over a sealed existing cohort, never new clear authority.

The private directory must outlive the job/container. Unknown outcomes retain the
intent and require reconciliation; opening the directory never replays a write.
Schema069 and the existing retirement owners remain the database authority.
"""

from collections import Counter
from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path


class ReconciliationRequired(RuntimeError):
    pass


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _check(condition, code):
    if not condition:
        raise ValueError(code)


def seal_inventory(plans):
    plans = deepcopy(plans)
    _check(isinstance(plans, list) and bool(plans), "inventory_empty")
    keys, scopes = [], set()
    for plan in plans:
        unsigned = {k: v for k, v in plan.items() if k != "proof_sha256"}
        _check(plan.get("proof_sha256") == digest(unsigned), "plan_digest")
        _check(
            plan.get("contract") == "recall.retirement-cohort.v1"
            and len(plan["parents"]) == 1,
            "singleton_required",
        )
        (entry,) = plan["parents"]
        source = entry["source_id"]
        _check(
            entry.get("catalog_created_at") and entry.get("manifest"),
            "catalog_identity_required",
        )
        _check(plan["scope"]["source_ids"] == [source], "plan_scope")
        keys.append((source, entry["native_parent_id"]))
        scopes.add((plan["scope"]["tenant_id"], plan["scope"]["principal_id"]))
    _check(
        len(keys) == len(set(keys)) and len(scopes) == 1,
        "inventory_scope_or_duplicates",
    )
    plans.sort(
        key=lambda p: (
            p["parents"][0]["source_id"],
            p["parents"][0]["native_parent_id"],
        )
    )
    inventory = dict(contract="recall.finite-retirement-inventory.v1", plans=plans)
    return dict(inventory, sha256=digest(inventory))


KNOWN_STATUSES = frozenset(
    {
        "observed_enabled",
        "enrolled",
        "preserved",
        "already_enabled",
        "disabled",
        "pending",
        "manifest_budget",
        "archive_budget",
        "represented",
        "projection_pending",
        "document_budget",
        "empty",
        "invalid_location",
        "locator_partial",
        "publication_failed",
        "metadata_timeout",
        "metadata_lock_busy",
        "catalog_changed",
        "owner_refused",
    }
)
KNOWN_STATUSES |= frozenset(
    "postpublication_" + s
    for s in (
        "already_enabled",
        "disabled",
        "pending",
        "manifest_budget",
        "archive_budget",
    )
)


def _validate_outcome(outcome):
    _check(
        outcome.get("certainty") in ("known", "unattempted", "unknown"),
        "outcome_certainty",
    )
    if outcome["certainty"] == "known":
        _check(outcome.get("status") in KNOWN_STATUSES, "outcome_status")


def _mkdir(path, *, exist_ok=False):
    """Make a private journal directory and persist its containing directory entry."""
    if not path.parent.exists():
        _mkdir(path.parent, exist_ok=True)
    path.mkdir(mode=0o700, exist_ok=exist_ok)
    descriptor = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write(path, value):
    """Create-once file, fsync its bytes and parent directory before returning."""
    data = canonical(value) + b"\n"
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as file:
        file.write(data)
        file.flush()
        os.fsync(file.fileno())
    # No overwrite, including an unexpected existing operator result.
    os.link(temporary, path)
    temporary.unlink()
    directory = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class Campaign:
    def __init__(self, directory, inventory, expected_sha256):
        checked = seal_inventory(inventory["plans"])
        _check(
            checked == inventory and checked["sha256"] == expected_sha256,
            "inventory_digest",
        )
        self.directory = Path(directory)
        self.inventory = checked
        self.plans = checked["plans"]
        self.sources = sorted({p["parents"][0]["source_id"] for p in self.plans})
        self.by_source = {
            s: [p for p in self.plans if p["parents"][0]["source_id"] == s]
            for s in self.sources
        }
        self.lock = None

    def __enter__(self):
        _mkdir(self.directory, exist_ok=True)
        _check(
            self.directory.stat().st_mode & 0o077 == 0,
            "private_state_directory_required",
        )
        self.lock = (self.directory / "lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            path = self.directory / "inventory.json"
            if not path.exists():
                _write(path, self.inventory)
            _check(json.loads(path.read_bytes()) == self.inventory, "inventory_changed")
            self._load()
            return self
        except BaseException:
            self.lock.close()
            raise

    def __exit__(self, *args):
        self.lock.close()

    def _next(self):
        for offset in range(len(self.sources)):
            index = (self.turn + offset) % len(self.sources)
            source = self.sources[index]
            if self.cursors[source] < len(self.by_source[source]):
                return index, self.by_source[source][self.cursors[source]]
        return None

    def _selection(self, limit):
        _check(type(limit) is int and 1 <= limit <= 25, "quantum_limit")
        selected = self._next()
        if selected is None:
            return None
        index, _ = selected
        source = self.sources[index]
        return index, self.by_source[source][
            self.cursors[source] : self.cursors[source] + limit
        ]

    def _accept(self, intent, result, index):
        outcomes = result["outcomes"]
        _check(
            isinstance(outcomes, list) and len(outcomes) <= len(intent["plans"]),
            "outcome_prefix",
        )
        for number, outcome in enumerate(outcomes):
            _validate_outcome(outcome)
            _check(
                outcome["certainty"] == "known" or number == len(outcomes) - 1,
                "nonterminal_outcome",
            )
        self.turn = (index + 1) % len(self.sources)
        self.chain = result["sha256"]
        self.quanta += 1
        self.pending = None
        for plan, outcome in zip(intent["plans"], outcomes):
            self.results.append(dict(plan=plan, outcome=outcome))
            if outcome["certainty"] == "known":
                self.cursors[plan["parents"][0]["source_id"]] += 1
            elif outcome["certainty"] == "unknown":
                self.pending = intent

    def _load(self):
        self.cursors = dict.fromkeys(self.sources, 0)
        self.turn = self.quanta = 0
        self.results = []
        self.pending = None
        self.chain = self.inventory["sha256"]
        directories = sorted(p for p in self.directory.iterdir() if p.is_dir())
        for number, directory in enumerate(directories):
            _check(directory.name == f"{number:08d}", "journal_sequence")
            if not (directory / "intent.json").exists():
                self.pending = dict(status="incomplete_intent", sequence=number)
                break
            intent = json.loads((directory / "intent.json").read_bytes())
            expected = self._selection(len(intent["plans"]))
            _check(expected is not None, "journal_overflow")
            index, plans = expected
            _check(
                intent
                == dict(
                    sequence=number,
                    inventory_sha256=self.inventory["sha256"],
                    previous_sha256=self.chain,
                    plans=plans,
                ),
                "journal_intent",
            )
            result_path = directory / "result.json"
            reconciled = directory / "reconciliation.json"
            observed = directory / "observed-state.json"
            _check(
                sum(p.exists() for p in (result_path, reconciled, observed)) <= 1,
                "journal_double_result",
            )
            if observed.exists():
                result_path = observed
            _check(
                not (result_path.exists() and reconciled.exists()),
                "journal_double_result",
            )
            if reconciled.exists():
                result_path = reconciled
            if not result_path.exists():
                self.pending = intent
                break
            result = json.loads(result_path.read_bytes())
            unsigned = {k: v for k, v in result.items() if k != "sha256"}
            _check(
                result.get("sha256") == digest(unsigned)
                and result.get("intent_sha256") == digest(intent),
                "journal_result",
            )
            self._accept(intent, result, index)
            if self.pending:
                break
        if self.pending:
            _check(directory == directories[-1], "journal_after_unknown")

    def quantum(self, owner, limit=25):
        if self.pending:
            raise ReconciliationRequired("retirement_outcome_requires_reconciliation")
        selected = self._selection(limit)
        if selected is None:
            return None
        index, plans = selected
        directory = self.directory / f"{self.quanta:08d}"
        directory.mkdir(mode=0o700)
        intent = dict(
            sequence=self.quanta,
            inventory_sha256=self.inventory["sha256"],
            previous_sha256=self.chain,
            plans=plans,
        )
        _write(directory / "intent.json", intent)
        descriptor = os.open(self.directory, os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self.pending = intent
        events = []

        def emit(name, value):
            event = dict(name=name, value=deepcopy(value))
            _write(directory / f"event-{len(events):05d}.json", event)
            events.append(event)

        outcomes = owner(deepcopy(plans), emit)
        # Validate without advancing, then commit one atomic quantum result.
        _check(
            isinstance(outcomes, list) and len(outcomes) <= len(plans), "outcome_prefix"
        )
        for number, outcome in enumerate(outcomes):
            _validate_outcome(outcome)
            _check(
                outcome["certainty"] == "known" or number == len(outcomes) - 1,
                "nonterminal_outcome",
            )
        result = dict(intent_sha256=digest(intent), outcomes=outcomes)
        result["sha256"] = digest(result)
        _write(directory / "result.json", result)
        self._accept(intent, result, index)
        return result

    def reconcile(self, inspect, *, observed=False):
        """Recover missing transport results without replaying the original owner."""
        _check(self.pending and "plans" in self.pending, "no_reconcilable_intent")
        intent = self.pending
        directory = self.directory / f"{intent['sequence']:08d}"
        _check(not (directory / "result.json").exists(), "owner_outcome_unknown")
        events = [
            json.loads(p.read_bytes()) for p in sorted(directory.glob("event-*.json"))
        ]
        count = len(events)

        def emit(name, value):
            nonlocal count
            _write(directory / f"event-{count:05d}.json", dict(name=name, value=value))
            count += 1

        evidence = None
        if observed:
            evidence = inspect(deepcopy(intent["plans"]), deepcopy(events), emit)
            outcomes = evidence["parents"]
            _check(
                isinstance(outcomes, list)
                and len(outcomes) == len(intent["plans"])
                and all(
                    row.get("plan_sha256") == plan["proof_sha256"]
                    and row.get("status") == "observed_enabled"
                    and row.get("certainty") == "known"
                    for row, plan in zip(outcomes, intent["plans"])
                ),
                "observed_parent_membership",
            )
        else:
            outcomes = inspect(deepcopy(events), emit)
        _check(
            isinstance(outcomes, list) and len(outcomes) <= len(intent["plans"]),
            "outcome_prefix",
        )
        for number, outcome in enumerate(outcomes):
            _validate_outcome(outcome)
            _check(outcome["certainty"] != "unknown", "owner_outcome_unknown")
            _check(
                outcome["certainty"] == "known" or number == len(outcomes) - 1,
                "nonterminal_outcome",
            )
        result = dict(intent_sha256=digest(intent), outcomes=outcomes, reconciled=True)
        result["sha256"] = digest(result)
        if observed:
            result.update(original_outcome="unknown", observed_state=evidence)
            result["sha256"] = digest(
                {k: v for k, v in result.items() if k != "sha256"}
            )
        _write(
            directory / ("observed-state.json" if observed else "reconciliation.json"),
            result,
        )
        self._load()
        return result

    def step(self, owner):
        result = self.quantum(lambda plans, emit: [owner(plans[0], emit)], limit=1)
        return None if result is None else self.results[-1]

    def report(self):
        counts = Counter(r["outcome"]["status"] for r in self.results)
        deferred = sum(
            r["outcome"]["certainty"] == "known"
            and r["outcome"]["status"]
            not in (
                "enrolled",
                "observed_enabled",
                "preserved",
                "already_enabled",
                "postpublication_already_enabled",
            )
            for r in self.results
        )
        done = sum(self.cursors.values())
        return dict(
            inventory_sha256=self.inventory["sha256"],
            considered_parents=done,
            remaining_parents=len(self.plans) - done,
            inventory_pass_exhausted=done == len(self.plans),
            deferred_parents=deferred,
            outcomes=dict(counts),
            reconciliation_required=bool(self.pending),
            original_outcomes_unknown=counts.get("observed_enabled", 0),
            retirement_complete=None,
            physical_reclaimed_bytes=None,
        )
