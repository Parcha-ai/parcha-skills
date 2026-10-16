"""Host-persisted once-only Render maintenance jobs; no mutation retries.

Matches the supervised-chain lifecycle: preflight, durable intent, one POST,
terminal status, complete logs, verified result. An exception leaves the campaign
intent unresolved. Reconciliation inspects that exact job; never repeat the POST.
"""

import ast
import base64
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
import re
from pathlib import Path
import time
import urllib.parse
import urllib.request
import uuid

from .retirement_campaign import _check, canonical, digest

DIAGNOSTIC = "recall_retirement_job_v1"
TERMINAL = {"succeeded", "failed", "canceled"}
REQUEST_TIMEOUT_SECONDS = 40
LOG_MAX_PAGES = 64
LOG_MAX_ROWS = 60000
LOG_MAX_BYTES = 64 * 1024**2
LOG_READ_SECONDS = 90


def encode_result(value, pin):
    raw = canonical(value)
    encoded = base64.b64encode(gzip.compress(raw)).decode()
    chunks = [encoded[start : start + 1800] for start in range(0, len(encoded), 1800)]
    return [
        dict(diagnostic=DIAGNOSTIC, part=n, data=part) for n, part in enumerate(chunks)
    ] + [
        dict(
            diagnostic=DIAGNOSTIC,
            finished=True,
            parts=len(chunks),
            sha256=hashlib.sha256(raw).hexdigest(),
            pin=pin,
        )
    ]


def decode_result(rows, pin):
    finished = [row for row in rows if row.get("finished")]
    _check(
        len(finished) == 1 and finished[0]["pin"] == pin,
        "remote_result_missing_or_wrong_pin",
    )
    end = finished[0]
    parts = [row for row in rows if "part" in row]
    _check(
        len(parts) == end["parts"]
        and [p["part"] for p in parts] == list(range(end["parts"])),
        "remote_result_parts",
    )
    raw = gzip.decompress(
        base64.b64decode("".join(row["data"] for row in parts), validate=True)
    )
    _check(hashlib.sha256(raw).hexdigest() == end["sha256"], "remote_result_digest")
    return json.loads(raw)


def _result_is_pending(rows, pin):
    """Recognize incomplete encoder frames; all other snapshots go to the decoder.

    Each GET is independent. Never combine parts from different observations,
    or catch a decoder failure and assume that more logs will repair it.
    """
    ends = [row for row in rows if row.get("finished")]
    parts = [row for row in rows if "part" in row]
    if len(ends) > 1 or (ends and ends[0].get("pin") != pin):
        return False
    if len(parts) + len(ends) != len(rows):
        return False
    if ends:
        end = ends[0]
        if (set(end) != {"diagnostic", "finished", "parts", "sha256", "pin"}
                or end["finished"] is not True or type(end["parts"]) is not int
                or not 1 <= end["parts"] <= 1000
                or not isinstance(end["sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", end["sha256"])):
            return False
        if len(parts) >= end["parts"]:
            return False
    indices = []
    for part in parts:
        if (set(part) != {"diagnostic", "part", "data"}
                or type(part["part"]) is not int or part["part"] < 0
                or not isinstance(part["data"], str) or not part["data"]):
            return False
        try:
            base64.b64decode(part["data"], validate=True)
        except ValueError:
            return False
        indices.append(part["part"])
    if ends:
        return indices == sorted(set(indices)) and all(n < ends[0]["parts"] for n in indices)
    return indices == list(range(len(indices)))


def request(method, path, body=None):
    data = canonical(body) if body is not None else None
    headers = {
        "Authorization": "Bearer " + os.environ["TASK_RECALL_RENDER_KEY"],
        "Content-Type": "application/json",
    }
    with urllib.request.urlopen(
        urllib.request.Request(
            "https://api.render.com/v1" + path,
            data=data,
            headers=headers,
            method=method,
        ),
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as response:
        return json.load(response)


def checkpoint_guard(config):
    path = Path(config["checkpoint_file"])
    raw = path.read_bytes()
    _check(
        hashlib.sha256(raw).hexdigest() == config["checkpoint_sha256"],
        "checkpoint_digest",
    )
    value = json.loads(raw)
    _check(0 <= time.time() - value["observed_epoch"] <= 120, "checkpoint_stale")
    _check(
        all(value[key] for key in ("ready", "health_ok", "receipt_ok", "search_ok")),
        "serving_or_provider_failed",
    )
    _check(
        value["replicas"] == 2
        and len(value["nodes"]) == 3
        and value["lag_seconds"] <= 10,
        "provider_shape_or_lag",
    )
    _check(value["fleet"] == config["expected_fleet"], "fleet_changed")
    _check(
        all(
            n["capacity_bytes"] == config["capacity_bytes"]
            and n["free_bytes"] >= config["minimum_free_bytes"]
            for n in value["nodes"]
        ),
        "capacity_or_headroom",
    )
    return dict(
        checkpoint_sha256=config["checkpoint_sha256"],
        observed_epoch=value["observed_epoch"],
    )


class RenderJob:
    def __init__(
        self,
        config,
        *,
        request=request,
        guard=None,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        self.config, self.request = config, request
        self.guard = guard or (lambda: checkpoint_guard(config))
        self.clock, self.sleep = clock, sleep
        self.base = "/services/" + config["service_id"]

    def run(self, body, pin, emit):
        preflight_deadline = self.clock() + 60

        def read(path):
            _check(self.clock() < preflight_deadline, "preflight_deadline")
            value = self.request("GET", path)
            _check(self.clock() < preflight_deadline, "preflight_deadline")
            return value

        checkpoint = self.guard()
        deployment = read(self.base + "/deploys?limit=1")[0]["deploy"]
        _check(
            deployment["status"] == "live"
            and deployment["commit"]["id"] == self.config["commit"],
            "deployment_changed",
        )
        service = read(self.base)
        _check(service.get("autoDeploy") == "no", "auto_deploy_enabled")
        # Render filters jobs by status. Any returned row blocks, including
        # terminal rows from an ignored filter; no later page could admit us.
        for status in ("pending", "running"):
            query = {"limit": 100, "status": status}
            jobs = read(self.base + "/jobs?" + urllib.parse.urlencode(query))
            _check(isinstance(jobs, list), "job_inventory_shape")
            _check(not jobs, "active_job")
        checkpoint = self.guard()  # No stale health after checking the job inventory.
        deployment = read(self.base + "/deploys?limit=1")[0]["deploy"]
        _check(
            deployment["status"] == "live"
            and deployment["commit"]["id"] == self.config["commit"],
            "deployment_changed_before_post",
        )
        body_sha = hashlib.sha256(body.encode()).hexdigest()
        emit(
            "launch_intent",
            dict(
                pin=pin,
                body_sha256=body_sha,
                config_sha256=digest(self.config),
                owner_id=service["ownerId"],
                **checkpoint,
            ),
        )
        encoded = base64.b64encode(gzip.compress(body.encode())).decode()
        command = (
            'python -u -c exec(__import__("gzip").decompress(__import__("base64").b64decode("'
            + encoded
            + '")))'
        )
        # Exactly one POST. A timeout or lost response is not safe to retry.
        _check(self.clock() < preflight_deadline, "preflight_deadline")
        job = self.request("POST", self.base + "/jobs", {"startCommand": command})
        _check(job.get("id") and job.get("createdAt"), "post_outcome_unknown")
        job = {k: job.get(k) for k in ("id", "createdAt", "status")}
        emit("job", job)
        deadline = self.clock() + 360
        while True:
            _check(self.clock() < deadline, "job_outcome_unknown")
            status = self.request("GET", self.base + "/jobs/" + job["id"])
            _check(status.get("id") == job["id"], "job_identity")
            emit("job_status", {k: status.get(k) for k in ("id", "status", "exitCode")})
            if status["status"] in TERMINAL:
                break
            self.sleep(5)
        _check(status["status"] == "succeeded", "job_failed_requires_reconciliation")
        return self._read_result(job, pin, service["ownerId"], emit, deadline=deadline)

    def reconcile(self, job_id, pin, owner_id, emit):
        """Inspect one existing job. No launch, polling, or mutation retry."""
        _check(isinstance(job_id, str) and bool(job_id), "job_identity")
        job = self.request(
            "GET", self.base + "/jobs/" + urllib.parse.quote(job_id, safe="")
        )
        _check(job.get("id") == job_id and job.get("createdAt"), "job_identity")
        _check(job.get("status") == "succeeded", "job_failed_requires_reconciliation")
        return self._read_result(job, pin, owner_id, emit)

    def _read_result(self, job, pin, owner_id, emit, *, deadline=None):
        while True:
            rows = self._read_result_rows(job, owner_id, deadline=deadline)
            if deadline is not None:
                _check(self.clock() < deadline, "remote_result_unavailable_before_deadline")
            if deadline is None or not _result_is_pending(rows, pin):
                break
            self.sleep(5)
        result = decode_result(rows, pin)
        emit(
            "verified_job_result",
            dict(pin=pin, result_sha256=digest(result), result=result),
        )
        return result

    def _read_result_rows(self, job, owner_id, *, deadline=None):
        stop = self.clock() + LOG_READ_SECONDS
        if deadline is not None:
            stop = min(stop, deadline)
        query = dict(
            ownerId=owner_id, resource=job["id"], type="app", direction="forward",
            limit=1000, startTime=job["createdAt"],
            endTime=datetime.now(timezone.utc).isoformat(),
        )
        rows, identities, windows = [], {}, set()
        count = size = 0
        bounds = None
        for _ in range(LOG_MAX_PAGES):
            # Every page shares the same deadline and original job scope.
            _check(self.clock() + REQUEST_TIMEOUT_SECONDS < stop,
                   "remote_result_unavailable_before_deadline")
            logs = self.request("GET", "/logs?" + urllib.parse.urlencode(query))
            _check(self.clock() < stop, "remote_result_unavailable_before_deadline")
            _check(isinstance(logs, dict) and isinstance(logs.get("logs"), list), "logs_shape")
            for row in logs["logs"]:
                _check(isinstance(row, dict) and isinstance(row.get("message"), str), "logs_shape")
                message = row["message"]
                raw = message.encode()
                count += 1
                size += len(raw)
                _check(count <= LOG_MAX_ROWS and size <= LOG_MAX_BYTES, "logs_bound")
                identity = (row.get("id"), row.get("timestamp"))
                if all(isinstance(value, str) and value for value in identity):
                    message_hash = hashlib.sha256(raw).digest()
                    if identity in identities:
                        _check(identities[identity] == message_hash, "logs_duplicate_conflict")
                        continue
                    identities[identity] = message_hash
                if DIAGNOSTIC in message:
                    event = json.loads(message[message.index("{"):])
                    _check(event["diagnostic"] == DIAGNOSTIC, "log_contract")
                    rows.append(event)
            if logs.get("hasMore") is False:
                return rows
            _check(logs.get("hasMore") is True, "logs_shape")
            start, end = logs.get("nextStartTime"), logs.get("nextEndTime")
            try:
                if bounds is None:
                    bounds = tuple(datetime.fromisoformat(query[key]) for key in ("startTime", "endTime"))
                    windows.add(bounds)
                window = tuple(datetime.fromisoformat(value) for value in (start, end))
                _check(all(value.tzinfo is not None for value in (*bounds, *window)), "logs_window")
                _check(bounds[0] <= window[0] <= window[1] <= bounds[1]
                       and window not in windows, "logs_window")
            except (TypeError, ValueError):
                raise ValueError("logs_window") from None
            windows.add(window)
            query.update(startTime=start, endTime=end)
        raise ValueError("logs_bound")


def package_body(config, payload):
    """Ship only new adapters into an existing pinned runtime; owners stay pinned."""
    required = {
        "recall_server." + name
        for name in (
            "retirement_runner",
            "chunk_retirement",
            "parent_chunk_proof",
            "chunk_bodies",
            "streaming_locators",
            "locator_backfill_plan",
        )
    }
    _check(
        set(config["owner_modules"]) == required
        and all(
            isinstance(v, str) and len(v) == 64
            for v in config["owner_modules"].values()
        ),
        "owner_module_pins",
    )
    root = Path(__file__).parent
    names = ["retirement_campaign.py", "retirement_enrollment.py", "retirement_jobs.py"]
    names += [
        "retirement_enrollment_owners/" + n
        for n in ("__init__.py", "metadata.py", "operator.py", "owner_helpers.py")
    ]
    sources = {name: (root / name).read_text() for name in names}
    spec = dict(
        invocation_id=uuid.uuid4().hex,
        commit=config["commit"],
        service_id=config["service_id"],
        owner_modules=config["owner_modules"],
        payload=payload,
        sources=sources,
    )
    pin = digest(spec)
    source = (
        "SPEC="
        + repr(spec)
        + "\nPIN="
        + repr(pin)
        + "\n"
        + """
import hashlib, importlib, json, os, signal, tempfile, time
from pathlib import Path
assert os.environ.get("RENDER_GIT_COMMIT") == SPEC["commit"]
assert os.environ.get("RENDER_SERVICE_ID") == SPEC["service_id"]
assert os.environ.get("RECALL_CHUNK_BODY_READS") == "archive"
assert os.environ.get("RECALL_SEARCH_PLANE") == "turbopuffer"
for name, expected in SPEC["owner_modules"].items():
    module = importlib.import_module(name)
    assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == expected
os.environ["PGOPTIONS"] = "-c statement_timeout=20000 -c lock_timeout=1000 -c idle_in_transaction_session_timeout=10000"
os.environ["PGCONNECT_TIMEOUT"] = "3"
def interrupted(*args):
    raise TimeoutError("retirement_job_deadline")
signal.signal(signal.SIGALRM, interrupted)
signal.alarm(295)
import recall_server
with tempfile.TemporaryDirectory() as temporary:
    directory = Path(temporary)
    for name, content in SPEC["sources"].items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    recall_server.__path__.insert(0, str(directory))
    from recall_server.db import BrainStore, applied_migration_versions
    from recall_server.retirement_jobs import encode_result
    from recall_server.retirement_enrollment import EnrollmentOwner, capture_page, observe_enrollment
    from recall_server.retirement_runner import RetirementScope
    from recall_server.archive_runtime import build_evidence_archive_store
    store = BrainStore(os.environ["RECALL_DATABASE_URL"], pool_max_size=4)
    try:
        with store.connect() as connection:
            assert 69 in applied_migration_versions(connection)
        payload = SPEC["payload"]
        if payload["operation"] == "capture":
            scope = RetirementScope(payload["tenant"], payload["principal"], (payload["source"],))
            result = capture_page(store, scope=scope, after=payload.get("after"), limit=payload["limit"], deadline_at=time.monotonic()+260)
        elif payload["operation"] == "observe":
            result = observe_enrollment(store, payload["plans"], deadline_at=time.monotonic()+260)
        else:
            assert payload["operation"] == "enroll"
            archive = build_evidence_archive_store(deadline_reads=True)
            assert archive is not None
            owner = EnrollmentOwner(store, archive, deadline_at=time.monotonic()+260)
            result = owner.quantum(payload["plans"], lambda *args: None)
        for event in encode_result(result, PIN):
            print(json.dumps(event), flush=True)
    finally:
        store.close()
        signal.alarm(0)
"""
    )
    compile(source, "retirement-render-job", "exec")
    return source, pin


def reconcile_enrollment(config, job_id, events, emit):
    """Bind an explicit job inspection to the persisted launch, not today's code."""
    launches = [e["value"] for e in events if e["name"] == "launch_intent"]
    _check(len(launches) == 1, "reconciliation_launch_missing")
    launch = launches[0]
    _check(launch["config_sha256"] == digest(config), "reconciliation_config_changed")
    jobs = [e["value"]["id"] for e in events if e["name"] == "job"]
    _check(not jobs or jobs == [job_id], "reconciliation_job_changed")
    emit("reconciliation_job", dict(id=job_id, pin=launch["pin"]))
    return RenderJob(config).reconcile(job_id, launch["pin"], launch["owner_id"], emit)


def verify_original_job(config, job_id, plans, events):
    """GET the exact terminal attempt and inspect pinned code without executing it."""
    launches = [e["value"] for e in events if e["name"] == "launch_intent"]
    _check(len(launches) == 1, "reconciliation_launch_missing")
    launch = launches[0]
    _check(launch["config_sha256"] == digest(config), "reconciliation_config_changed")
    jobs = [e["value"]["id"] for e in events if e["name"] == "job"]
    _check(not jobs or jobs == [job_id], "reconciliation_job_changed")
    job = request(
        "GET",
        "/services/"
        + config["service_id"]
        + "/jobs/"
        + urllib.parse.quote(job_id, safe=""),
    )
    _check(
        job.get("id") == job_id and job.get("status") == "succeeded",
        "original_job_not_succeeded",
    )
    prefix = 'python -u -c exec(__import__("gzip").decompress(__import__("base64").b64decode("'
    suffix = '")))'
    command = job.get("startCommand", "")
    _check(
        command.startswith(prefix) and command.endswith(suffix), "original_job_command"
    )
    encoded = command[len(prefix) : -len(suffix)]
    _check(
        re.fullmatch(r"[A-Za-z0-9+/=]+", encoded) is not None, "original_job_command"
    )
    body = gzip.decompress(base64.b64decode(encoded, validate=True))
    _check(
        hashlib.sha256(body).hexdigest() == launch["body_sha256"], "original_job_body"
    )
    lines = body.decode().splitlines()
    _check(
        lines[0].startswith("SPEC=") and lines[1].startswith("PIN="),
        "original_job_spec",
    )
    spec, pin = ast.literal_eval(lines[0][5:]), ast.literal_eval(lines[1][4:])
    _check(
        pin == launch["pin"]
        and digest(spec) == pin
        and spec["payload"] == dict(operation="enroll", plans=plans),
        "original_job_scope",
    )
    return dict(
        id=job_id, pin=pin, body_sha256=launch["body_sha256"], status="succeeded"
    )


def reconcile_observed_enrollment(
    original_config, current_config, job_id, plans, events, emit
):
    """Inspect the write attempt, then launch only a bounded read-only observation."""
    original = verify_original_job(original_config, job_id, plans, events)
    _check(
        original_config["service_id"] == current_config["service_id"],
        "observation_service_changed",
    )
    emit("original_job_body_verified", original)
    body, pin = package_body(current_config, dict(operation="observe", plans=plans))
    observation_job = dict(
        pin=pin,
        body_sha256=hashlib.sha256(body.encode()).hexdigest(),
        commit=current_config["commit"],
    )

    def record(name, value):
        emit("observation_" + name, value)
        if name == "job":
            observation_job["id"] = value["id"]

    result = RenderJob(current_config).run(body, pin, record)
    _check(
        result.get("read_only") is True and result.get("archive_calls") == 0,
        "observation_contract",
    )
    return dict(
        original_job=original,
        observation_job=observation_job,
        parents=result["parents"],
    )


class RemoteEnrollmentOwner:
    def __init__(self, config):
        self.config = config

    def __call__(self, plans, emit):
        body, pin = package_body(self.config, dict(operation="enroll", plans=plans))
        launched = False

        def record(name, value):
            nonlocal launched
            emit(name, value)
            if name == "launch_intent":
                launched = True

        try:
            return RenderJob(self.config).run(body, pin, record)
        except Exception as error:
            if launched:
                raise
            return [
                dict(
                    status="remote_preflight_refused",
                    certainty="unattempted",
                    error_class=type(error).__name__,
                )
            ]
