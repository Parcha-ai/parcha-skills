"""Task evidence survives continuation without claiming Hermes run ownership."""
from copy import deepcopy
import importlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest


PATH = Path(__file__).resolve().parents[1] / 'runtime/plugin_next/demo.py'
SPEC = importlib.util.spec_from_file_location('task_flow_demo', PATH)
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class Computer:
    def __init__(self):
        self.session_id = 'exact-native-session'
        self.lock = threading.Lock()
        self.last_used = time.monotonic()
        self.prompts = []

    def send(self, prompt):
        self.prompts.append(prompt)

    def read_result(self, timeout):
        return {'type': 'result', 'session_id': self.session_id, 'is_error': False,
                'result': 'I inspected the current task and will continue the work.'}

    def alive(self):
        return True

    def terminate(self):
        pass


class Reader:
    def __init__(self):
        self.raw = {'reference': {'board': 'project', 'task_id': 't_owner'},
            'task': {'id': 't_owner', 'title': 'Fix parser', 'status': 'running', 'body': 'Handle empty input'},
            'comments': [{'id': 1, 'task_id': 't_owner', 'author': 'reviewer',
                          'body': 'Artifact rev-a needs an empty-input fix', 'created_at': 1}]}

    def read(self, ref):
        return deepcopy(self.raw)


class FlowFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='tether-task-flow-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        scope = demo._core()
        self.core = scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        self.reader, self.computer, self.sent = Reader(), Computer(), []
        self.open_store()
        self.binding = self.slice.bind(source_kind='claude_session', session_id=self.computer.session_id,
            cwd=str(self.root), team_id='TTEST', channel_id='CTEST', thread_ts='100.1',
            owner_user_id='UHUMAN', spawned=True)
        self.addCleanup(self.close_store)

    def open_store(self):
        self.store = self.core.store.Store(self.root / 'tether.db')
        settings = self.core.active.ActiveSettings(enabled=True, launcher='direct', presence=False)
        self.driver = self.core.session_driver.SessionDriver(self.store, self.root / 'session', settings,
            launch_plan=demo.forbidden, child_env=demo.forbidden, popen=demo.forbidden)
        self.driver._session_for = lambda *args, **kwargs: self.computer
        self.slice = self.core.active.ActiveSlice(runtime=self.store, driver=self.driver, settings=settings,
            descriptor=SimpleNamespace(authorized_owner_ids=('UHUMAN',), workspace_id='TTEST'),
            egress=lambda channel, thread, text: self.sent.append((channel, thread, text)), task_reader=self.reader)
        self.slice.herdr_factory = demo.forbidden

    def close_store(self):
        self.driver.shutdown()
        self.store.close()

    def link(self):
        return self.slice.handle({'op': 'task_link', 'bridge_id': self.binding['binding_id'],
            'task_ref': {'board': 'project', 'task_id': 't_owner'},
            'expected_generation': self.store.task_binding(self.binding['binding_id'])['generation']})

    def admit(self, text='Continue the work'):
        count = self.store._db.execute('SELECT count(*) FROM turns').fetchone()[0]
        return self.slice.claim({'workspace': 'TTEST', 'channel': 'CTEST', 'thread': '100.1',
            'actor': 'UHUMAN', 'message_id': f'200.{count:06d}'}, text)

    def latest(self):
        row = self.store._db.execute('SELECT attempt_id FROM attempts ORDER BY rowid DESC').fetchone()
        return self.store.attempt_context(row['attempt_id'])


class TaskFlowTest(FlowFixture):
    def test_cli_links_and_shows_task_through_real_private_broker(self):
        broker = importlib.import_module(self.core.active.__package__ + '.broker')
        socket = self.root / 'bridge.sock'
        server = broker.BrokerServer(socket, self.slice.handle)
        server.start()
        self.addCleanup(server.stop)
        cli = PATH.parents[2] / 'bin/tether.js'
        env = {'HOME': str(self.root), 'PATH': os.environ['PATH'], 'LANG': 'C.UTF-8',
               'TETHER_HERDR': 'off', 'TETHER_BROKER_SOCKET': str(socket)}
        def call(*args):
            result = subprocess.run(['node', str(cli), 'task', *args], env=env,
                text=True, capture_output=True, timeout=5, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            return json.loads(result.stdout)
        linked = call('link', '--bridge-id', self.binding['binding_id'], '--board', 'project',
            '--task-id', 't_owner', '--expected-generation', '1')
        self.assertEqual(linked['binding']['task_ref'], {'board': 'project', 'task_id': 't_owner'})
        shown = call('show', '--bridge-id', self.binding['binding_id'])
        self.assertEqual(shown['binding']['generation'], 2)
        self.assertIn('empty-input fix', shown['task_context']['data']['comments'][0]['body'])
        self.assertEqual(self.computer.prompts, [], 'Link/show do not start a computer')

    def test_current_review_reaches_original_session_after_restart(self):
        self.assertEqual(self.link()['binding']['generation'], 2)
        self.admit('Review the current artifact')
        self.assertEqual(self.slice.run_once(), 1)
        first = self.latest()
        shown = self.slice.op_task_show({'bridge_id': self.binding['binding_id']})
        self.assertFalse(shown['context_changed_since_last_attempt'])
        self.assertEqual(shown['last_attempt']['context_digest'], first['task_context']['digest'])
        self.assertIn('Artifact rev-a needs an empty-input fix', self.computer.prompts[-1])
        self.reader.raw['comments'].append({'id': 2, 'task_id': 't_owner', 'author': 'reviewer',
            'body': 'Artifact rev-b changed. Re-review rev-b and preserve zero values.', 'created_at': 2})
        self.assertTrue(self.slice.op_task_show({'bridge_id': self.binding['binding_id']})['context_changed_since_last_attempt'])
        self.close_store()
        self.open_store()
        self.assertEqual(self.store.task_binding(self.binding['binding_id'])['task_ref'], first['task_ref'])
        self.admit('Human decision: preserve the public API')
        self.slice.run_once()
        second = self.latest()
        self.assertNotEqual(first['attempt_id'], second['attempt_id'])
        self.assertEqual(first['task_ref'], second['task_ref'])
        self.assertEqual(first['source']['session_id'], second['source']['session_id'])
        self.assertNotEqual(first['task_context']['digest'], second['task_context']['digest'])
        prompt = self.computer.prompts[-1]
        self.assertIn('Re-review rev-b and preserve zero values', prompt)
        self.assertLess(prompt.index('Current task context'), prompt.index('Human decision:'))
        self.assertEqual(self.reader.raw['task']['status'], 'running')
        self.assertEqual(len(self.sent), 2)

    def test_unlinked_thread_never_reads_task_service(self):
        self.reader.read = demo.forbidden
        self.admit()
        self.slice.run_once()
        self.assertEqual(len(self.computer.prompts), 1)
        self.assertIsNone(self.latest()['task_context'])

    def test_failed_read_never_starts_computer_or_leaks_error(self):
        self.link()
        def fail(ref):
            raise RuntimeError('PRIVATE_BACKEND_ERROR')
        self.reader.read = fail
        self.admit()
        self.slice.run_once()
        self.assertEqual(self.computer.prompts, [])
        self.assertEqual(self.latest()['state'], 'failed')
        self.assertEqual(self.latest()['error_code'], 'task_context_unavailable')
        self.assertIsNone(self.latest()['task_context'])
        self.assertIn('coding session was not started', self.sent[-1][2])
        self.assertNotIn('PRIVATE_BACKEND_ERROR', self.sent[-1][2])

    def test_wrong_task_and_oversized_context_prevent_execution(self):
        self.link()
        for wrong in (True, False):
            self.reader.raw['task'] = {'id': 't_other' if wrong else 't_owner', 'body': 'x' * 40000}
            self.admit()
            self.slice.run_once()
            self.assertEqual(self.latest()['state'], 'failed')
        self.assertEqual(self.computer.prompts, [])

    def test_generation_and_busy_turns_fence_reassociation(self):
        self.link()
        with self.assertRaisesRegex(self.core.store.StoreError, 'current binding'):
            self.store.set_task_ref(self.binding['binding_id'], None, expected_generation=1)
        self.admit()
        with self.assertRaisesRegex(self.core.store.StoreError, 'queued or running'):
            self.store.set_task_ref(self.binding['binding_id'], None, expected_generation=2)
        self.slice.run_once()
        result = self.slice.handle({'op': 'task_unlink', 'bridge_id': self.binding['binding_id'], 'expected_generation': 2})
        self.assertEqual(result['binding']['generation'], 3)
        self.assertIsNone(result['binding']['task_ref'])
        self.assertEqual(self.latest()['task_ref']['task_id'], 't_owner')

    def test_stop_and_binding_change_during_preparation_prevent_execution(self):
        self.link()
        original = self.reader.read
        def stop(ref):
            self.slice._stop.set()
            return original(ref)
        self.reader.read = stop
        self.admit()
        self.slice.run_once()
        self.assertEqual(self.latest()['error_code'], 'task_context_interrupted')
        self.slice._stop.clear()
        def change(ref):
            self.store._db.execute('UPDATE bindings SET generation=generation+1')
            self.store._db.commit()
            return original(ref)
        self.reader.read = change
        self.admit()
        self.slice.run_once()
        self.assertEqual(self.latest()['error_code'], 'task_context_stale')
        self.assertEqual(self.computer.prompts, [])

    def test_snapshot_cannot_be_overwritten(self):
        self.link()
        self.admit()
        attempt = self.store.schedule_next(self.binding['endpoint_id'])
        ref = self.core.active.TaskRef('project', 't_owner')
        snapshot = self.slice._read_task(ref)
        self.store.save_task_context(attempt['attempt_id'], snapshot)
        self.store.save_task_context(attempt['attempt_id'], snapshot)
        self.reader.raw['task']['body'] = 'A newer snapshot'
        with self.assertRaisesRegex(self.core.store.StoreError, 'task_context_already_recorded'):
            self.store.save_task_context(attempt['attempt_id'], self.slice._read_task(ref))
        self.assertEqual(self.latest()['task_context']['digest'], snapshot.digest)

    def test_old_store_upgrade_retains_binding(self):
        path = self.root / 'old.sqlite'
        with sqlite3.connect(path) as connection:
            connection.executescript(self.core.store._SCHEMA)
            connection.execute("INSERT INTO endpoints VALUES('end_old','native-old','native','claude_session','{}','ready','x','x')")
            connection.execute("INSERT INTO bindings VALUES('bnd_old','end_old','TTEST','CTEST','10.1','UHUMAN','old','active',1,'x','x')")
        store = self.core.store.Store(path)
        try:
            self.assertIsNone(store.task_binding('bnd_old')['task_ref'])
            self.assertEqual(store.task_binding('bnd_old')['endpoint_id'], 'end_old')
        finally:
            store.close()

    def test_unlinked_show_does_not_require_host_service(self):
        self.slice.task_reader = None
        self.assertIsNone(self.slice.handle({'op': 'task_show', 'bridge_id': self.binding['binding_id']})['task_context'])
        with self.assertRaisesRegex(self.core.active.BrokerRefused, 'Configure an explicit'):
            self.link()
        self.assertIsNone(self.store.task_binding(self.binding['binding_id'])['task_ref'])


class ActualHermesFlowTest(FlowFixture):
    def test_actual_task_refresh_and_transport_do_not_mutate_host(self):
        try:
            canonical = importlib.import_module('hermes_cli.kanban_db')
        except ImportError:
            self.skipTest('Optional actual Hermes task API is not installed')
        path = self.root / 'actual-host.sqlite'
        with sqlite3.connect(path) as connection:
            connection.executescript(canonical.SCHEMA_SQL)
            connection.execute("INSERT INTO tasks(id,title,status,tenant,created_at) VALUES('t_owner','Fix parser','running','tenant-a',1)")
            connection.execute("INSERT INTO task_comments(task_id,author,body,created_at) VALUES('t_owner','reviewer','Artifact rev-a has an empty-input bug',1)")
        provider = importlib.import_module(self.core.active.__package__ + '.hermes_tasks')
        self.slice.task_reader = provider.HermesTaskReader({'project': path}, tenant='tenant-a')
        self.link()
        before = path.read_bytes()
        self.admit()
        self.slice.run_once()
        first = self.latest()
        self.assertIn('Artifact rev-a has an empty-input bug', self.computer.prompts[-1])
        self.assertEqual(path.read_bytes(), before)
        with sqlite3.connect(path) as connection:
            connection.execute("INSERT INTO task_comments(task_id,author,body,created_at) VALUES('t_owner','reviewer','Artifact rev-b fixes empty input; re-review zero values',2)")
        before = path.read_bytes()
        self.admit('Continue in the same session')
        self.slice.run_once()
        self.assertIn('re-review zero values', self.computer.prompts[-1])
        self.assertNotEqual(first['task_context']['digest'], self.latest()['task_context']['digest'])
        self.assertEqual(first['task_ref'], self.latest()['task_ref'])
        self.assertEqual(first['source']['session_id'], self.latest()['source']['session_id'])
        self.assertEqual(path.read_bytes(), before)
        with sqlite3.connect(path) as connection:
            self.assertEqual(connection.execute("SELECT status FROM tasks WHERE id='t_owner'").fetchone()[0], 'running')


if __name__ == '__main__':
    unittest.main()
