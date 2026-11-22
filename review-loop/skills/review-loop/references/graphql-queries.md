# GitHub GraphQL and REST queries for review-loop

All calls go through `gh api`, which uses the caller's existing authentication.

## Fetch review threads with resolution state (paginated)

```bash
gh api graphql -F owner=OWNER -F name=REPO -F number=PR_NUMBER -f query='
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          comments(first: 5) {
            nodes {
              id
              body
              author { login }
              createdAt
              updatedAt
            }
          }
        }
      }
    }
  }
}'
```

Repeat with `-F cursor=<endCursor>` while `hasNextPage` is true. The first comment's
`author.login` identifies the reviewer (`greptile-apps[bot]`, `devin-ai-integration[bot]`, or
a human login). Filter on `isResolved == false` for open threads; `isOutdated` threads still
count as open until resolved.

## Batch-resolve threads

```bash
gh api graphql -f query='
mutation {
  t1: resolveReviewThread(input: {threadId: "ID1"}) { thread { isResolved } }
  t2: resolveReviewThread(input: {threadId: "ID2"}) { thread { isResolved } }
}'
```

Assert that every alias in the response has `thread.isResolved == true`. Batch up to 20
aliases per request.

## Reply on an inline thread

Reply to the thread's first comment through REST so the reply lands inside the thread:

```bash
gh api --method POST "repos/{owner}/{repo}/pulls/<PR_NUMBER>/comments/<COMMENT_DATABASE_ID>/replies" \
  -f body='Fixed in <sha>: <what changed and why>.'
```

The REST `comments` endpoint returns the numeric `id` needed here:

```bash
gh api --paginate "repos/{owner}/{repo}/pulls/<PR_NUMBER>/comments?per_page=100" \
  --jq '.[] | {id, path, line, user: .user.login, in_reply_to_id, body}'
```

## Reviews (approve, request changes, comment)

```bash
gh api --paginate "repos/{owner}/{repo}/pulls/<PR_NUMBER>/reviews?per_page=100" \
  --jq '.[] | {id, user: .user.login, state, submitted_at, body}'
```

Re-request a human or app reviewer after a push:

```bash
gh pr edit <PR_NUMBER> --add-reviewer <login>
```

## Issue comments edited in place

General PR comments are issue comments. Greptile updates one summary comment repeatedly, so
select by `updated_at`, not `created_at`:

```bash
gh api --paginate "repos/{owner}/{repo}/issues/<PR_NUMBER>/comments?per_page=100" \
  | jq -s 'add
    | map(select(.user.login | test("greptile"; "i")))
    | sort_by(.updated_at)
    | last
    | {author: .user.login, updated_at, body}'
```

## Sticky comments edited in place

A top-level comment that reports state (a QA report, a merge summary, this loop's report)
exists once per pull request and kind. Its first line is `<!-- agent-sticky:<kind> -->`.
Find it and edit it; create it only when none exists. The comment belongs to the PR, not
to the agent that wrote it: `ROSTER` is the JSON list of agent identities that may own it, so
an agent that takes over a PR edits the existing comment. It defaults to the authenticated
identity alone (`BOT_LOGIN`, from `gh api graphql -f query='{viewer{login}}' --jq
.data.viewer.login`). `HEAD_SHA` is the commit the body describes.

```bash
ROSTER="${ROSTER:-[\"$BOT_LOGIN\"]}"
MARKER="<!-- agent-sticky:$KIND -->"

# Freshness guard: never write a body for a head the PR has moved past.
PR_HEAD="$(gh pr view <PR_NUMBER> --json headRefOid --jq .headRefOid)"
[ "$HEAD_SHA" = "$PR_HEAD" ] || { echo "stale: body is for $HEAD_SHA, PR head is $PR_HEAD"; exit 1; }

{ printf '%s\n' "$MARKER"; cat "$BODY_FILE"; } > "$BODY_FILE.sticky"

# The oldest matching comment is the canonical one.
COMMENT_ID="$(gh api --paginate "repos/{owner}/{repo}/issues/<PR_NUMBER>/comments?per_page=100" \
  --jq ".[] | select((.user.login | IN($ROSTER[])) and (.body | startswith(\"$MARKER\"))) | .id" \
  | head -n1)"

if [ -n "$COMMENT_ID" ]; then
  gh api --method PATCH "repos/{owner}/{repo}/issues/comments/$COMMENT_ID" \
    -F body=@"$BODY_FILE.sticky" --jq .html_url
else
  gh pr comment <PR_NUMBER> --body-file "$BODY_FILE.sticky"
fi
```

- Search every page right before writing. Do not reuse an ID read earlier.
- Verify after every write: list the matching comments again and re-read the PR head.
  - If the head moved, stop; the writer for the new head owns the next update.
  - If more than one matching comment exists (two writers created at once), the oldest stays
    and must carry the body whose `Head:` equals the PR head; copy it in if needed. Delete
    this identity's own extras with
    `gh api --method DELETE "repos/{owner}/{repo}/issues/comments/<id>"` and collapse other
    identities' extras as `OUTDATED`. Never delete another account's comment.
  - If the oldest comment's `Head:` is not the PR head, a concurrent writer replaced a
    current body with a stale one; rewrite it once with this body.
- If an edit of another identity's comment is refused, create this identity's own and
  collapse the other as `OUTDATED`.
- Do not use `gh pr comment --edit-last`: it edits the identity's latest comment of any kind,
  which may be a trigger.
- An edit notifies nobody. Do not add an `@mention` to an edit and expect it to notify.

## Collapse earlier trigger comments

After the triggered review finishes, minimize the earlier triggers as outdated. A trigger is
any comment by the roster that mentions `@greptile-apps` and is not a sticky, so older prose
triggers (`@greptile-apps please review again …`) are included. The last line of the list is
the latest trigger, which stays visible:

```bash
gh api --paginate "repos/{owner}/{repo}/issues/<PR_NUMBER>/comments?per_page=100" \
  --jq ".[] | select((.user.login | IN($ROSTER[])) and (.body | test(\"@greptile-apps\")) and (.body | startswith(\"<!-- agent-sticky:\") | not)) | .node_id" \
  | sed '$d' \
  | while read -r NODE_ID; do
      gh api graphql -f id="$NODE_ID" -f query='
        mutation($id: ID!) {
          minimizeComment(input: {subjectId: $id, classifier: OUTDATED}) {
            minimizedComment { isMinimized }
          }
        }' --jq .data.minimizeComment.minimizedComment.isMinimized
    done
```

Collapsing keeps the comment and its history; deleting would not.

## Greptile run in progress

```bash
gh pr checks <PR_NUMBER> --json name,state \
  | jq -r '.[] | select(.name | test("greptile"; "i")) | .state'

HEAD_SHA=$(gh pr view <PR_NUMBER> --json headRefOid -q .headRefOid)
gh api "repos/{owner}/{repo}/commits/$HEAD_SHA/check-runs" \
  --jq '.check_runs[] | select(.name | test("greptile"; "i")) | {status, conclusion}'
```

Post the trigger comment only when no run is `PENDING`, `IN_PROGRESS`, `queued`, or
`in_progress`, or when the only such run is hung (see "Hung run: use or replace the summary"):

```bash
gh pr comment <PR_NUMBER> --body "@greptile-apps review"
```

## Bounded poll for the check run

The check-run poll ends when the run completes, when it turns hung (still in progress more
than 20 minutes after `started_at`), or after 10 minutes with no Greptile check run at all.
A started run is never cut off before it can turn hung.

```bash
HUNG_AFTER_SECONDS=1200
NO_RUN_TIMEOUT_SECONDS=600
POLL_INTERVAL_SECONDS=10
POLL_STARTED=$(date +%s)
HUNG=0
while true; do
  CHECK=$(gh api "repos/{owner}/{repo}/commits/$HEAD_SHA/check-runs" \
    --jq '[.check_runs[] | select(.name | test("greptile"; "i"))] | last // empty' 2>/dev/null)
  if [ -z "$CHECK" ]; then
    if [ $(( $(date +%s) - POLL_STARTED )) -ge "$NO_RUN_TIMEOUT_SECONDS" ]; then
      echo "Timed out: no Greptile check run after about 10 minutes." >&2
      exit 1
    fi
    sleep "$POLL_INTERVAL_SECONDS"; continue
  fi
  STATUS=$(echo "$CHECK" | jq -r '.status // "completed"')
  if [ "$STATUS" = "completed" ]; then
    break
  fi
  RUN_AGE=$(echo "$CHECK" | jq -r 'if .started_at then (now - (.started_at | fromdateiso8601) | floor) else 0 end')
  if [ "$RUN_AGE" -gt "$HUNG_AFTER_SECONDS" ]; then
    HUNG=1
    break
  fi
  sleep "$POLL_INTERVAL_SECONDS"
done
```

## Hung run: use or replace the summary

When `HUNG=1`, the check run will never complete. Read the summary's "Last reviewed commit".
If it already equals the head, its score is the result. Otherwise post one trigger and poll
the summary for up to 10 minutes from the trigger.

```bash
summary_for_head() {
  gh api --paginate "repos/{owner}/{repo}/issues/<PR_NUMBER>/comments?per_page=100" \
    | jq -s -r --arg head "$HEAD_SHA" 'add
      | map(select(.user.login | test("greptile"; "i")))
      | sort_by(.updated_at) | last // empty
      | select(.body | test("Last reviewed commit: \\[[^\\]]*\\]\\([^)]*/commit/" + $head + "\\)"))
      | select(.body | test("[0-5]/5|greptile_confidence_score:[0-5]"))
      | .updated_at'
}

if [ "$HUNG" = 1 ] && [ -z "$(summary_for_head)" ]; then
  gh pr comment <PR_NUMBER> --body "@greptile-apps review"
  TRIGGER_POSTED=$(date +%s)
  until [ -n "$(summary_for_head)" ]; do
    if [ $(( $(date +%s) - TRIGGER_POSTED )) -ge 600 ]; then
      echo "Timed out waiting for Greptile's replacement summary after about 10 minutes." >&2
      exit 1
    fi
    sleep "$POLL_INTERVAL_SECONDS"
  done
fi
```

On timeout, stop the loop and report; never continue with stale or missing results.
