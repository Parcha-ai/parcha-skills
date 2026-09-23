#!/usr/bin/env python3
"""Explicit root-only Linux sandbox regression, with synthetic local objects.

No archive/provider/DB/network calls. The tool fixture proves executable handoff,
not real DuckDB query correctness. Run with sudo using the test Python interpreter.
"""
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]
from recall_server.deep_inspection import AgentExecObject, _agent_exec_command  # noqa: E402


ATTACK = r"""
import ctypes,errno,json,pathlib,subprocess,sys
private=pathlib.Path(sys.argv[1]); admitted=pathlib.Path(sys.argv[2]); operation=sys.argv[3]
libc=ctypes.CDLL(None,use_errno=True)
code=0; syscall_errno=None
if operation=='syscall_lazy':
 code=libc.umount2(b'/mnt/archil/evidence',2)
 syscall_errno=ctypes.get_errno()
elif operation=='syscall_remount':
 code=libc.mount(None,b'/tmp/recall-authorized',None,32|4096,None)
 syscall_errno=ctypes.get_errno()
else:
 command=(['mount','-o','remount,bind,rw','/tmp/recall-authorized'] if operation=='remount'
  else ['umount', '-l' if operation=='lazy' else '-R', '/mnt/archil/evidence'])
 result=subprocess.run(command,capture_output=True,timeout=5)
 code=result.returncode
try:
 exposed=private.read_bytes()==b'synthetic-private'
except OSError as error:
 assert error.errno in (errno.ENOENT,errno.EACCES,errno.EPERM),error.errno
 exposed=False
write_errno=None
if 'remount' in operation:
 try:
  admitted.chmod(0o600)
  with admitted.open('ab') as f:f.write(b'UNEXPECTED-WRITE')
 except OSError as error:write_errno=error.errno
 assert write_errno in (errno.EROFS,errno.EACCES,errno.EPERM),write_errno
assert code!=0,(operation,'mount operation succeeded')
assert not exposed,'private object exposed'
print(json.dumps(dict(operation=operation,denied=True,errno=syscall_errno,write_errno=write_errno)))
"""

PROGRAM = r"""python3 - <<'PY'
import concurrent.futures,json,pathlib,subprocess
caps={line.split(':')[0]:line.split(':',1)[1].strip()
 for line in pathlib.Path('/proc/self/status').read_text().splitlines()
 if line.startswith(('CapInh:','CapPrm:','CapEff:','CapBnd:','CapAmb:','NoNewPrivs:'))}
assert caps['NoNewPrivs']=='1',caps
assert all(int(caps[key],16)==0 for key in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb')),caps
admitted='/tmp/recall-authorized/'+ALLOWED
assert pathlib.Path(admitted).read_bytes()==b'synthetic-allowed'
assert pathlib.Path('/mnt/archil/evidence/'+ALLOWED).read_bytes()==b'synthetic-allowed'
assert not pathlib.Path('/mnt/archil/evidence/'+PRIVATE).exists()
assert subprocess.check_output(['bash','-c',"printf pipe-ok | cat"],text=True)=='pipe-ok'
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
 assert list(pool.map(abs,(-1,-2)))==[1,2]
if SCAN:
 assert subprocess.check_output(['duckdb','-c','select 1'],text=True).strip()=='1'
route_lines=pathlib.Path('/proc/net/route').read_text().splitlines()
assert len(route_lines)<=1,route_lines
if route_lines: assert route_lines[0].split()[0]=='Iface',route_lines
checks=0
for namespace in ('same','mapped','unmapped'):
 for operation in ('lazy','recursive','remount','syscall_lazy','syscall_remount'):
  cmd=['python3','-c',ATTACK,'/mnt/archil/evidence/'+PRIVATE,admitted,operation]
  if namespace!='same':
   cmd=['unshare','--user',*(['--map-root-user'] if namespace=='mapped' else []),'--mount','--fork',*cmd]
  result=subprocess.run(cmd,capture_output=True,text=True,timeout=10)
  if result.returncode:
   # Namespace creation itself may be refused. An inner assertion/crash is
   # not a security pass and cannot be hidden behind a nonzero status.
   assert namespace!='same' and not result.stdout,result.stderr
   assert result.stderr.startswith('unshare:') and any(
    value in result.stderr.lower() for value in ('operation not permitted','permission denied')),result.stderr
  else:
   assert json.loads(result.stdout)['denied'] is True
  checks+=1
print(json.dumps(dict(checks=checks,caps=caps,ordinary_shell_and_threads=True,network_routes=0)))
PY
"""


def main():
    if os.geteuid() != 0:
        raise SystemExit("This optional kernel regression requires explicit root invocation")
    made = []
    try:
        # Mount targets only; each is hidden by private tmpfs before the wrapper.
        for name in ("/docs", "/datasets"):
            path = Path(name)
            if not path.exists():
                path.mkdir(mode=0o755)
                made.append(path)
        with tempfile.TemporaryDirectory(prefix="recall-namespace-test-") as tmp:
            evidence = Path(tmp) / "evidence"
            evidence.mkdir(mode=0o755)

            def put(body):
                digest = hashlib.sha256(body).hexdigest()
                key = f"objects/{digest[:2]}/{digest}"
                path = evidence / key
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(body)
                return AgentExecObject(key, digest)

            allowed = put(b"synthetic-allowed")
            private = put(b"synthetic-private")
            tool = put(b'#!/bin/sh\nprintf "1\\n"\n')
            results = []
            for scan in (False, True):
                program = PROGRAM.replace("ALLOWED", repr(allowed.object_key)).replace(
                    "PRIVATE", repr(private.object_key)
                ).replace("ATTACK", repr(ATTACK)).replace("SCAN", repr(scan))
                command = _agent_exec_command(
                    program=program, objects=(allowed, tool) if scan else (allowed,),
                    document_aliases={}, record_spans={}, routing_receipts={}, timeout_seconds=30,
                    dataset_aliases={allowed.object_key: "s1/2026-09/documents-part-00000.parquet"} if scan else None,
                    tool_objects={"linux-x86_64": tool, "linux-arm64": tool} if scan else None,
                    allow_missing_objects=scan,
                )
                prelude = (
                    "set -eu\nmount --make-rprivate /\n"
                    "mount -t tmpfs tmpfs /docs\nmount -t tmpfs tmpfs /datasets\n"
                    "mount -t tmpfs tmpfs /mnt\nmkdir -p /mnt/archil/evidence\n"
                    "mount --bind " + shlex.quote(str(evidence)) + " /mnt/archil/evidence\n"
                    "mount -t tmpfs tmpfs /tmp\n"
                )
                result = subprocess.run(
                    ["unshare", "--mount", "--fork", "bash", "-s"],
                    input=prelude + command, text=True, capture_output=True, timeout=60,
                )
                assert result.returncode == 0, (scan, result.stdout, result.stderr)
                proof = json.loads(result.stdout)
                assert proof["checks"] == 15
                results.append({"scan": scan, **proof})
        print(json.dumps({"status": "pass", "synthetic_only": True, "results": results}))
    finally:
        for path in reversed(made):
            path.rmdir()


if __name__ == "__main__":
    main()
