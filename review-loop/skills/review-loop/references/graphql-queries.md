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
Find it and edit it; create it only when none exists. `BOT_LOGIN` is the authenticated
identity (`gh api graphql -f query='{viewer{login}}' --jq .data.viewer.login`).

```bash
MARKER="<!-- agent-sticky:$KIND -->"
{ printf '%s\n' "$MARKER"; cat "$BODY_FILE"; } > "$BODY_FILE.sticky"

COMMENT_ID="$(gh api --paginate "repos/{owner}/{repo}/issues/<PR_NUMBER>/comments?per_page=100" \
  --jq ".[] | select(.user.login == \"$BOT_LOGIN\" and (.body | startswith(\"$MARKER\"))) | .id" \
  | head -n1)"

if [ -n "$COMMENT_ID" ]; then
  gh api --method PATCH "repos/{owner}/{repo}/issues/comments/$COMMENT_ID" \
    -F body=@"$BODY_FILE.sticky" --jq .html_url
else
  gh pr comment <PR_NUMBER> --body-file "$BODY_FILE.sticky"
fi
```

- Search every page right before writing. Do not reuse an ID read earlier.
- If a create races another writer and two comments carry this identity's marker, keep the
  oldest, copy the newer body into it, and delete the newer one with
  `gh api --method DELETE "repos/{owner}/{repo}/issues/comments/<id>"`. Delete only this
  identity's own comments.
- Do not use `gh pr comment --edit-last`: it edits the identity's latest comment of any kind,
  which may be a trigger.
- An edit notifies nobody. Do not add an `@mention` to an edit and expect it to notify.

## Collapse earlier trigger comments

After the triggered review finishes, minimize this identity's earlier `@greptile-apps`
comments as outdated. The last line of the list is the latest trigger, which stays visible:

```bash
gh api --paginate "repos/{owner}/{repo}/issues/<PR_NUMBER>/comments?per_page=100" \
  --jq ".[] | select(.user.login == \"$BOT_LOGIN\" and (.body | startswith(\"@greptile-apps\"))) | .node_id" \
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
`in_progress`:

```bash
gh pr comment <PR_NUMBER> --body "@greptile-apps review"
```

## Bounded poll for the check run

```bash
ATTEMPTS=0
MAX_ATTEMPTS=60
POLL_INTERVAL_SECONDS=10
while true; do
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -gt "$MAX_ATTEMPTS" ]; then
    echo "Timed out waiting for the Greptile check run after about 10 minutes." >&2
    exit 1
  fi
  CHECK=$(gh api "repos/{owner}/{repo}/commits/$HEAD_SHA/check-runs" \
    --jq '.check_runs[] | select(.name | test("greptile"; "i"))' 2>/dev/null)
  if [ -z "$CHECK" ]; then
    sleep "$POLL_INTERVAL_SECONDS"; continue
  fi
  STATUS=$(echo "$CHECK" | jq -r '.status // "completed"')
  if [ "$STATUS" = "completed" ]; then
    break
  fi
  sleep "$POLL_INTERVAL_SECONDS"
done
```

On timeout, stop the loop and report; never continue with stale or missing results.
