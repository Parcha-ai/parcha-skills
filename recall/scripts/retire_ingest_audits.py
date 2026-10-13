#!/usr/bin/env python3
"""One reviewed audit page per durable Render operation; never automatic replay."""
import argparse
import json
from pathlib import Path
import re
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'server'))
from recall_server.retirement_campaign import _check, _mkdir, _write, digest
from recall_server.retirement_jobs import RenderJob, reconcile_enrollment


def package_body(config, payload):
    required = {'recall_server.' + name for name in ('audit_retirement','archive','archive_runtime','db')}
    _check(set(config['owner_modules']) == required and all(
        isinstance(v,str) and re.fullmatch('[0-9a-f]{64}',v)
        for v in config['owner_modules'].values()), 'owner_module_pins')
    _check(config.get('cutoff_sealed') is True, 'writer_cutoff_not_sealed')
    _check(re.fullmatch('[0-9a-f]{40}',config['commit']) is not None, 'runtime_commit')
    scope = config['scope']
    _check(set(scope) == {'tenant_id','source_id','cutoff'}, 'audit_scope')
    operation = payload['operation']
    _check(operation in {'archive','clear','inspect','restore'}, 'audit_operation')
    if operation == 'archive':
        _check(set(payload) == {'operation','after','limit'}, 'archive_payload')
    else:
        _check(set(payload) == {'operation','receipt'}, 'receipt_payload')
        _check(all(payload['receipt'].get(k) == v for k,v in scope.items()), 'receipt_scope')
    spec = dict(config=config,payload=payload)
    pin = digest(spec)
    body = 'SPEC=' + repr(spec) + '\nPIN=' + repr(pin) + '\n' + '''
import hashlib, importlib, json, os, signal
from pathlib import Path
config, payload = SPEC['config'], SPEC['payload']
assert os.environ.get('RENDER_GIT_COMMIT') == config['commit']
assert os.environ.get('RENDER_SERVICE_ID') == config['service_id']
assert os.environ.get('RECALL_SEARCH_PLANE') == 'turbopuffer'
for name, expected in config['owner_modules'].items():
    module = importlib.import_module(name)
    assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == expected
os.environ['PGOPTIONS'] = '-c statement_timeout=15000 -c lock_timeout=1000 -c idle_in_transaction_session_timeout=10000'
os.environ['PGCONNECT_TIMEOUT'] = '3'
def interrupted(*args):
    raise TimeoutError('audit_job_deadline')
signal.signal(signal.SIGALRM, interrupted)
signal.alarm(295)
from recall_server.db import BrainStore
from recall_server.archive_runtime import build_evidence_archive_store
from recall_server.audit_retirement import AuditRetirement
from recall_server.retirement_jobs import encode_result
store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=4)
try:
    archive = build_evidence_archive_store(deadline_reads=True)
    assert archive is not None
    owner = AuditRetirement(store, archive)
    if payload['operation'] == 'archive':
        result = owner.archive_page(**config['scope'], after=payload['after'], limit=payload['limit'])
    else:
        result = getattr(owner, payload['operation'])(payload['receipt'])
    for event in encode_result(result, PIN):
        print(json.dumps(event), flush=True)
finally:
    store.close()
    signal.alarm(0)
'''
    compile(body,'audit-retirement-render-job','exec')
    return body,pin


def run_operation(config, payload, directory, expected_sha256):
    body,pin = package_body(config,payload)
    _check(pin == expected_sha256, 'reviewed_operation_digest')
    directory = Path(directory)
    _mkdir(directory)  # Exclusive creation prevents reopening/replaying an uncertain attempt.
    _write(directory/'intent.json',dict(config=config,payload=payload,sha256=pin))
    events = []
    def emit(name,value):
        _write(directory/f'event-{len(events):04d}.json',dict(name=name,value=value))
        events.append(name)
    try:
        result = RenderJob(config).run(body,pin,emit)
        _write(directory/'result.json',result)
        return result
    except BaseException as error:
        _write(directory/'failure.json',dict(error_class=type(error).__name__,
                                            reconciliation_required='launch_intent' in events))
        raise


def reconcile_operation(directory, job_id, expected_sha256):
    directory = Path(directory)
    intent = json.loads((directory/'intent.json').read_bytes())
    _check(intent['sha256'] == expected_sha256 == digest({k:intent[k] for k in ('config','payload')}),
           'reviewed_operation_digest')
    events = [json.loads(p.read_bytes()) for p in sorted(directory.glob('event-*.json'))]
    report = directory/('reconcile-' + uuid.uuid4().hex)
    _mkdir(report)
    counter = []
    def emit(name,value):
        _write(report/f'event-{len(counter):04d}.json',dict(name=name,value=value))
        counter.append(name)
    # The existing helper is transport-only: exact persisted pin/job identity,
    # GETs and result verification. It never executes an enrollment or a retry.
    result = reconcile_enrollment(intent['config'],job_id,events,emit)
    _write(report/'result.json',result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    parser.add_argument('--config',type=Path)
    parser.add_argument('--payload',type=Path)
    parser.add_argument('--expected-sha256',required=True)
    parser.add_argument('--reconcile-job')
    args = parser.parse_args()
    if args.reconcile_job:
        result = reconcile_operation(args.directory,args.reconcile_job,args.expected_sha256)
    else:
        _check(args.config is not None and args.payload is not None, 'operation_files_required')
        result = run_operation(json.loads(args.config.read_bytes()),json.loads(args.payload.read_bytes()),
                               args.directory,args.expected_sha256)
    print(json.dumps(result,sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps(dict(error_class=type(error).__name__)),file=sys.stderr)
        raise SystemExit(1) from None
