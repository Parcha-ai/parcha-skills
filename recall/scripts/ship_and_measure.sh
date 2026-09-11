#!/usr/bin/env bash
# Ship one PR and measure it: CI wait -> squash merge -> Render deploy -> systems card -> diff vs previous row.
# usage: recall/scripts/ship_and_measure.sh <pr-number> [dimensions] [truth-split]
#   dimensions default: availability,latency,accuracy,freshness,integrity
# Requires: gh auth (source ~/.github-app-auth.sh parcha), Render API key in the interactive 1Password env,
#   ~/.config/recall-brain/client.json with an owner read token, and the private truth set path in RECALL_TRUTH.
set -u
PR=${1:?pr number}
DIMS=${2:-availability,latency,accuracy,freshness,integrity}
SPLIT=${3:-validation}
REPO=Parcha-ai/parcha-skills
MCP_SERVICE=${RECALL_RENDER_MCP_SERVICE:-srv-d9o4vf6417fc73ei24ag}
TRUTH=${RECALL_TRUTH:-$HOME/.recall-cascade/agentic-map-reduce-20260727/employee-truth-v2-approved.jsonl}
OUT=${RECALL_CARD_OUT:-$HOME/.recall/systems-card/out}
PRIV=${RECALL_CARD_PRIVATE:-$HOME/.recall/systems-card}
ROOT=$(git rev-parse --show-toplevel)

timeout 1500 gh pr checks "$PR" --repo "$REPO" --watch --interval 30 >/dev/null 2>&1
if ! gh pr checks "$PR" --repo "$REPO" 2>&1 | grep -qE '^test\s+pass'; then echo "CI not green"; gh pr checks "$PR" --repo "$REPO"; exit 1; fi
gh pr merge "$PR" --repo "$REPO" --squash --delete-branch 2>&1 | tail -1
git -C "$ROOT" fetch -q origin main
echo "main=$(git -C "$ROOT" rev-parse --short origin/main)"

GREPPY_INTERACTIVE_ENVIRONMENT_ID="$(sudo bash -c 'source /etc/greppy-interactive/config; printf %s "$GREPPY_INTERACTIVE_ENVIRONMENT_ID"')"
export OP_SERVICE_ACCOUNT_TOKEN="$(sudo bash -c 'source /etc/greppy-interactive/config; cat "$GREPPY_INTERACTIVE_TOKEN_FILE"')"
RK="$(OP_CACHE=false op environment read "$GREPPY_INTERACTIVE_ENVIRONMENT_ID" 2>/dev/null | awk -F= '$1=="RENDER_API_KEY"{print substr($0,index($0,"=")+1);exit}')"
unset OP_SERVICE_ACCOUNT_TOKEN GREPPY_INTERACTIVE_ENVIRONMENT_ID
H="Authorization: Bearer $RK"
curl -s -m 30 -X POST -H "$H" -H 'Content-Type: application/json' "https://api.render.com/v1/services/$MCP_SERVICE/deploys" -d '{"clearCache":"do_not_clear"}' | jq -r '"deploy \(.id)"'
for _ in $(seq 1 40); do st=$(curl -s -m 20 -H "$H" "https://api.render.com/v1/services/$MCP_SERVICE/deploys?limit=1" | jq -r '.[].deploy | "\(.status) \(.commit.id[0:7])"'); case "$st" in live*|*failed*|canceled*) echo "$st"; break;; esac; sleep 20; done
unset RK H
sleep 60
cd "$ROOT/recall" && git -C "$ROOT" checkout -q --detach origin/main
python3 -m evals.systems_card run --output-dir "$OUT" --private-dir "$PRIV" --truth "$TRUTH" --truth-split "$SPLIT" --since "$(date -u -d '-10 days' +%Y-%m-%d)" --repetitions 3 --dimensions "$DIMS" 2>&1 | tail -1
python3 - "$OUT" <<'PY'
import json,os,sys
out=sys.argv[1]
rows=[json.loads(l) for l in open(os.path.join(out,'history.jsonl')) if l.strip()]
base,prev,now=rows[0],rows[-2] if len(rows)>1 else rows[0],rows[-1]
keys=sorted({k for r in (prev,now) for k in r if k not in ('generated_at','overall','gates_failed','git_sha')})
print(f"{'metric':60} {'baseline':>10} {'prev':>10} {'now':>10}")
for k in keys: print(f"{k:60} {str(base.get(k))[:10]:>10} {str(prev.get(k))[:10]:>10} {str(now.get(k))[:10]:>10}")
c=json.load(open(os.path.join(out,'card.json')))
print("gates failed:", [(p['name'],g['metric'],round(g['observed'],3) if isinstance(g['observed'],float) else g['observed']) for d in c['dimensions'].values() for p in d['probes'] for g in p['gates'] if g['passed'] is False])
PY
