"""One reviewed concurrent index operation; no provider transport or automatic retry."""
import hashlib
import json
import os
import re
import signal
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg
from psycopg.rows import dict_row

NAME = 'canonical_jobs_status_idx'
DEFINITION = 'CREATE INDEX canonical_jobs_status_idx ON public.canonical_ingest_jobs USING btree (tenant_id, status, updated_at)'
DROP_SQL = 'DROP INDEX CONCURRENTLY public.canonical_jobs_status_idx'
CREATE_SQL = DEFINITION.replace('CREATE INDEX ', 'CREATE INDEX CONCURRENTLY ', 1)
PRIMARY = 'CREATE UNIQUE INDEX canonical_ingest_jobs_pkey ON public.canonical_ingest_jobs USING btree (tenant_id, source_id, job_id)'
VOLATILE = {'physical_bytes', 'idx_scan', 'last_idx_scan'}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def inspect(connection):
    table = connection.execute("""SELECT oid,pg_get_userbyid(relowner) AS owner FROM pg_class
        WHERE oid=to_regclass('public.canonical_ingest_jobs') AND relkind='r'""").fetchone()
    rows = connection.execute("""SELECT c.oid,c.relname AS name,c.relkind,
        pg_get_userbyid(c.relowner) AS owner,pg_get_indexdef(c.oid) AS definition,
        pg_relation_size(c.oid) AS physical_bytes,i.indisunique,i.indisprimary,
        i.indisvalid,i.indisready,i.indislive,i.indisreplident,
        s.idx_scan,s.last_idx_scan
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        LEFT JOIN pg_stat_user_indexes s ON s.indexrelid=c.oid
        WHERE i.indrelid=to_regclass('public.canonical_ingest_jobs')
          AND (i.indisprimary OR c.relname='canonical_jobs_status_idx')
        ORDER BY c.oid LIMIT 3""").fetchall()
    primary = [r for r in rows if r['indisprimary']]
    targets = [r for r in rows if r['name'] == NAME]
    dependencies = connection.execute("""SELECT classid::regclass::text AS classid,objid,objsubid,
        refclassid::regclass::text AS refclassid,refobjid,refobjsubid,deptype
        FROM pg_depend WHERE
          (classid='pg_class'::regclass AND objid=to_regclass('public.canonical_jobs_status_idx')) OR
          (refclassid='pg_class'::regclass AND refobjid=to_regclass('public.canonical_jobs_status_idx'))
        ORDER BY classid,objid,objsubid,refclassid,refobjid,refobjsubid LIMIT 65""").fetchall()
    constraints = connection.execute("""SELECT oid FROM pg_constraint
        WHERE conindid=to_regclass('public.canonical_jobs_status_idx') LIMIT 2""").fetchall()
    if not table or len(primary) != 1 or len(targets) > 1 or len(rows) > 2 or len(dependencies) > 64:
        raise RuntimeError('catalog_shape')
    return dict(table=table,primary=primary[0],index=targets[0] if targets else None,
                dependencies=dependencies,constraints=constraints)


def contract(state):
    return {key: ({k:v for k,v in value.items() if k not in VOLATILE}
                  if key in ('primary','index') and value is not None else value)
            for key,value in state.items()}


def validate(state, *, allow_invalid=False):
    primary = state['primary']
    if (state['table']['owner'] != 'postgres' or primary['definition'] != PRIMARY
            or not all(primary[k] for k in ('indisprimary','indisunique','indisvalid','indisready','indislive'))):
        raise RuntimeError('primary_shape')
    index = state['index']
    if index is None:
        if state['dependencies'] or state['constraints']:
            raise RuntimeError('absent_index_dependencies')
        return
    if (index['definition'] != DEFINITION or index['relkind'] != 'i' or index['owner'] != 'postgres'
            or (not allow_invalid and not all(index[k] for k in ('indisvalid','indisready','indislive')))
            or any(index[k] for k in ('indisunique','indisprimary','indisreplident'))
            or state['constraints']):
        raise RuntimeError('index_shape')
    deps = state['dependencies']
    if len(deps) != 3 or any(d['classid'] != 'pg_class' or d['objid'] != index['oid']
            or d['objsubid'] != 0 or d['refclassid'] != 'pg_class'
            or d['refobjid'] != state['table']['oid'] or d['refobjsubid'] <= 0
            or d['deptype'] != 'a' for d in deps):
        raise RuntimeError('index_dependencies')


def execute(connection, *, action, expected, original_drop=None):
    if action not in ('drop','recreate','finish_drop') or connection.autocommit is not True:
        raise ValueError('explicit_autocommit_operation_required')
    before = inspect(connection)
    if contract(before) != contract(expected):
        raise RuntimeError('catalog_changed')
    validate(before,allow_invalid=action=='finish_drop')
    if action == 'finish_drop':
        if (not isinstance(original_drop,dict) or set(original_drop)!={'intent_sha256','terminal_job_id','state'}
                or re.fullmatch('[a-f0-9]{64}',original_drop['intent_sha256']) is None
                or not isinstance(original_drop['terminal_job_id'],str) or not original_drop['terminal_job_id']):
            raise RuntimeError('original_drop_required')
        original = original_drop['state']
        validate(original)
        if (before['index'] is None or original['index'] is None
                or all(before['index'][k] for k in ('indisvalid','indisready','indislive'))):
            raise RuntimeError('original_invalid_index_required')
        comparable = contract(before)
        comparable['index'] = dict(comparable['index'])
        for key in ('indisvalid','indisready','indislive'):
            comparable['index'][key] = original['index'][key]
        if comparable != contract(original):
            raise RuntimeError('original_index_changed')
    elif original_drop is not None:
        raise RuntimeError('unexpected_recovery_intent')
    if action == 'drop' and (before['index'] is None or before['index']['idx_scan'] != 0
                             or before['index']['last_idx_scan'] is not None):
        raise RuntimeError('unused_index_proof_required')
    if action == 'recreate' and before['index'] is not None:
        raise RuntimeError('recreate_requires_absence')
    result = dict(action=action,ddl_attempted=True,ddl_acknowledged=False)
    try:
        connection.execute(CREATE_SQL if action == 'recreate' else DROP_SQL)
        result['ddl_acknowledged'] = True
        after = inspect(connection)
        validate(after)
        if (contract(after)['primary'] != contract(before)['primary'] or after['table'] != before['table']
                or (after['index'] is None) != (action != 'recreate')):
            raise RuntimeError('postcondition_changed')
        return dict(result,status='recreated' if action == 'recreate' else 'dropped',state=after)
    except (Exception, KeyboardInterrupt) as error:
        return dict(result,status='inspect_required',outcome='unknown',error_class=type(error).__name__)


def main(config):
    signal.alarm(295)
    if (not isinstance(config,dict) or config.get('action') not in ('inspect','drop','recreate','finish_drop')
            or config.get('commit') != os.environ.get('RENDER_GIT_COMMIT')
            or config.get('service_id') != os.environ.get('RENDER_SERVICE_ID')):
        raise RuntimeError('reviewed_runtime_required')
    url = urlsplit(os.environ['RECALL_DATABASE_URL'])
    identity = {'host_sha256':hashlib.sha256((url.hostname or '').encode()).hexdigest(),
                'port':url.port or 5432,'database':url.path}
    if url.scheme not in ('postgres','postgresql') or digest(identity) != config['database_identity_sha256']:
        raise RuntimeError('database_changed')
    mutate = config['action'] != 'inspect'
    dsn = url.geturl()
    if mutate:
        credentials = json.loads(os.environ[config['credential_key']])
        authority = quote(credentials['username'],safe='')+':'+quote(credentials['password'],safe='')+'@'+url.hostname+':'+str(url.port or 5432)
        dsn = urlunsplit((url.scheme,authority,url.path,url.query,url.fragment))
    options = ('-c default_transaction_read_only='+('off' if mutate else 'on')+
               ' -c statement_timeout=270000 -c lock_timeout=1000 -c maintenance_work_mem=64MB -c max_parallel_maintenance_workers=0')
    with psycopg.connect(dsn,autocommit=True,connect_timeout=5,row_factory=dict_row,
                         options=options,application_name='recall-job-status-index') as connection:
        if mutate:
            connection.execute('SET ROLE postgres')
            if not connection.execute("SELECT has_schema_privilege(current_user,'public','CREATE') AS allowed").fetchone()['allowed']:
                raise RuntimeError('rollback_create_privilege_required')
            value = execute(connection,action=config['action'],expected=config['expected'],original_drop=config.get('original_drop'))
        else:
            connection.execute('SET statement_timeout=5000')
            value = dict(status='inspected',state=inspect(connection),read_only=True)
    print(json.dumps(dict(diagnostic='recall_job_status_index_operation_v1',**value),default=str),flush=True)
    signal.alarm(0)
    if value['status'] == 'inspect_required':
        raise SystemExit(2)


if __name__ == '__main__':
    main(globals().get('CONFIG'))
