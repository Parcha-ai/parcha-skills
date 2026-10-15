#!/usr/bin/env python3
"""Bounded audit pages per durable Render operation; never automatic replay."""
import argparse
import json
from pathlib import Path
import re
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'server'))
from recall_server.retirement_campaign import _check, _mkdir, _write, digest
from recall_server.retirement_jobs import RenderJob, reconcile_enrollment


def validate_receipts(scope, receipts):
    _check(isinstance(receipts,list) and 1 <= len(receipts) <= 10, 'batch_page_budget')
    seen = set()
    for index, receipt in enumerate(receipts):
        _check(isinstance(receipt,dict) and all(receipt.get(k) == v for k,v in scope.items()),
               'receipt_scope')
        _check(receipt.get('contract') == 'recall.ingest-audit-retirement.v1' and
               receipt.get('sha256') == digest({k:v for k,v in receipt.items() if k != 'sha256'}),
               'receipt_digest')
        _check(receipt['sha256'] not in seen, 'duplicate_receipt')
        seen.add(receipt['sha256'])
        _check(type(receipt['scanned']) is int and 0 <= receipt['scanned'] <= 50000 and
               type(receipt['row_count']) is int and 0 <= receipt['row_count'] <= receipt['scanned'],
               'page_row_budget')
        _check(type(receipt['raw_bytes']) is int and 0 <= receipt['raw_bytes'] <= 32*1024*1024,
               'page_byte_budget')
        _check(type(receipt['exhausted']) is bool and all(
            receipt[k] is None or isinstance(receipt[k],str) for k in ('after','next_after')),
            'page_cursor')
        _check((receipt['next_after'] != receipt['after']) if receipt['scanned'] else
               (receipt['next_after'] == receipt['after'] and receipt['exhausted']), 'page_progress')
        if index:
            previous = receipts[index-1]
            _check(not previous['exhausted'] and previous['next_after'] == receipt['after'],
                   'receipt_chain')


def require_durable_archive(config, payload):
    directory = Path(payload['archive_operation'])
    intent = json.loads((directory/'intent.json').read_bytes())
    _check(intent['sha256'] == digest({k:intent[k] for k in ('config','payload')}), 'archive_intent_digest')
    _check(intent['payload']['operation'] == 'archive_batch' and
           intent['config']['scope'] == config['scope'], 'archive_intent_scope')
    # A recovered result must come from the original GET-only reconciliation.
    paths = [directory/'result.json', *sorted(directory.glob('reconcile-*/result.json'))]
    results = [json.loads(path.read_bytes()) for path in paths if path.is_file()]
    _check(bool(results) and all(r.get('status') == 'archived' and
           r.get('receipts') == payload['receipts'] for r in results), 'durable_archive_receipts')
    for path in paths:
        if not path.is_file():
            continue
        result = json.loads(path.read_bytes())
        events = [json.loads(p.read_bytes()) for p in sorted(path.parent.glob('event-*.json'))]
        verified = [e['value'] for e in events if e['name'] == 'verified_job_result']
        _check(len(verified) == 1 and verified[0] == dict(
            pin=intent['sha256'], result_sha256=digest(result), result=result), 'archive_verified_result')
    receipts = payload['receipts']
    _check(receipts[0]['after'] == intent['payload']['after'] and
           len(receipts) <= intent['payload']['max_pages'] and
           all(r['scanned'] <= intent['payload']['limit'] for r in receipts), 'archive_requested_bounds')


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
    _check(operation in {'archive','clear','inspect','restore','archive_batch','clear_batch','inspect_batch'}, 'audit_operation')
    if operation in {'archive','archive_batch'}:
        keys = {'operation','after','limit'} | ({'max_pages'} if operation == 'archive_batch' else set())
        _check(set(payload) == keys, 'archive_payload')
        _check(type(payload['limit']) is int and 1 <= payload['limit'] <= 50000, 'page_row_budget')
        _check(payload['after'] is None or isinstance(payload['after'],str), 'page_cursor')
        if operation == 'archive_batch':
            _check(type(payload['max_pages']) is int and 1 <= payload['max_pages'] <= 10, 'batch_page_budget')
    elif operation in {'clear_batch','inspect_batch'}:
        keys = {'operation','receipts'} | ({'archive_operation'} if operation == 'clear_batch' else set())
        _check(set(payload) == keys, 'batch_payload')
        validate_receipts(scope,payload['receipts'])
        if operation == 'clear_batch':
            _check(isinstance(payload['archive_operation'],str) and bool(payload['archive_operation']),
                   'archive_operation_required')
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
    elif payload['operation'] == 'archive_batch':
        receipts, after = [], payload['after']
        for _ in range(payload['max_pages']):
            receipt = owner.archive_page(**config['scope'], after=after, limit=payload['limit'])
            receipts.append(receipt)
            after = receipt['next_after']
            if receipt['exhausted']:
                break
        result = dict(status='archived', receipts=receipts, next_after=after,
                      exhausted=receipts[-1]['exhausted'])
    elif payload['operation'] in {'clear_batch','inspect_batch'}:
        operation = payload['operation'].removesuffix('_batch')
        results = []
        for receipt in payload['receipts']:
            outcome = getattr(owner, operation)(receipt)
            results.append(dict(receipt_sha256=receipt['sha256'], outcome=outcome))
        result = dict(status=operation + '_finished', results=results)
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
    if payload['operation'] == 'clear_batch':
        require_durable_archive(config,payload)
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
