"""Reviewed suffix recovery stays explicit and uses the existing collector runtime."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from collector import cli


class SuffixRecoveryCli(unittest.TestCase):
    def invoke(self,command='recover',manifest=None,harness='claude'):
        argv=['collector',command,'--harness',harness,'--root','/synthetic','--source-id','claude:test','--spool','/synthetic/spool']
        if manifest is not None:argv.extend(['--verified-suffix-manifest',str(manifest)])
        return mock.patch('sys.argv',argv)

    def test_default_recover_still_uses_existing_dead_payload_owner(self):
        with mock.patch.dict('os.environ',{},clear=True),self.invoke(),mock.patch.object(cli,'Collector')as factory,contextlib.redirect_stdout(io.StringIO())as out:
            factory.return_value.recover_dead_payloads.return_value={'recovered':0,'unrecoverable':0}
            cli.main()
        factory.return_value.recover_dead_payloads.assert_called_once()
        factory.return_value.scan.assert_not_called();factory.return_value.flush.assert_not_called()
        self.assertEqual(json.loads(out.getvalue()),{'recovered':0,'unrecoverable':0})

    def test_manifest_preflight_runs_before_runtime_and_only_enqueues_reviewed_items(self):
        with tempfile.TemporaryDirectory()as temp:
            path=Path(temp)/'reviewed.json';items=[{'selected':'first'},{'selected':'second'}];path.write_text(json.dumps({'items':items}))
            with mock.patch.dict('os.environ',{},clear=True),self.invoke(manifest=path),mock.patch.object(cli,'Collector')as factory,mock.patch.object(cli,'inspect_suffix')as inspect,mock.patch.object(cli,'recover_suffix',return_value={'status':'queued'})as recover,contextlib.redirect_stdout(io.StringIO()):
                cli.main()
            self.assertEqual(inspect.call_args_list,[mock.call(Path('/synthetic'),item)for item in items])
            self.assertEqual(recover.call_args_list,[mock.call(factory.return_value,item)for item in items])
            factory.return_value.scan.assert_not_called();factory.return_value.flush.assert_not_called();factory.return_value.recover_dead_payloads.assert_not_called()
            factory.return_value.close.assert_called_once()

    def test_wrong_command_or_invalid_manifest_cannot_open_spool(self):
        with tempfile.TemporaryDirectory()as temp:
            path=Path(temp)/'reviewed.json';path.write_text('{"items":[]}')
            for command,harness in [('recover','claude'),('scan','claude'),('recover','codex')]:
                with self.subTest(command=command,harness=harness),mock.patch.dict('os.environ',{},clear=True),self.invoke(command,path,harness),mock.patch.object(cli,'Collector')as factory,contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):cli.main()
                    factory.assert_not_called()


if __name__=='__main__':unittest.main()
