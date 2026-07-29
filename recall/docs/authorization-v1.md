# Recall authorization v1

`recall.authorization.v1` separates identity from authority:

1. An OAuth/OIDC provider proves who is calling and issues an access token for
   the exact Recall MCP resource.
2. Recall verifies the token locally against the provider's HTTPS JWKS.
3. Recall resolves the external subject to its own brain, role, and source
   grants. Token claims never select or broaden a tenant.
4. A closed policy matrix authorizes each MCP operation and writes a
   content-free decision audit.

Descope is the hosted preset. Any standards-based OIDC provider that issues
RS256 access tokens with `iss`, `sub`, `aud`, `exp`, and `scope` claims can use
the same OSS boundary. Recall does not require a Descope management key and
does not maintain passwords.

## MCP policy

| MCP action | Human | Workload | Roles | Required scope |
|---|---:|---:|---|---|
| initialize, ping, tools/list | yes | yes | owner, admin, member | `read` |
| recall_search, recall_show, recall_related | yes | yes | owner, admin, member | `read` |
| recall_forget | yes | yes | owner, admin | `forget` |
| every unlisted operation | no | no | none | none |

Source-scoped collectors and webhooks remain separate workload credentials.
They cannot become teammate readers by sending tenant or principal fields.
Unauthorized tools are absent from discovery and return the same `unknown
tool` error when called, so denial does not reveal whether an object exists.

## Invitation lifecycle

The owner opens `/admin`, chooses a company brain, enters a teammate's email,
and shares the displayed endpoint:

```text
https://recall.example.com/mcp/brains/tenant:company
```

Recall stores a key-derived blind index for matching and an AES-256-GCM encrypted copy for
the owner-facing access ledger. It never stores the email in plaintext. On the
teammate's first MCP request, the client completes OAuth and presents an access
token. Recall accepts the invitation only when the provider asserts the exact
normalized email with `email_verified=true`. The provider subject—not the
email—becomes the durable identity.

Acceptance grants only company sources owned by current organization members.
Sources owned by unrelated principals in the same tenant remain invisible.
New sources from organization members are propagated to active invitees. A
revocation disables the external binding and removes brain and source grants in
one transaction; the next MCP request is denied.

Personal brains cannot be invited into. Pending invitations expire after seven
days, are single-use, and are unambiguous: if the same email has pending access
to multiple brains, the client must use the brain-specific MCP endpoint.

## Resource-server configuration

```text
RECALL_MCP_RESOURCE_URI=https://recall.example.com/mcp
RECALL_AUTHORIZATION_SERVERS=https://identity.example.com
RECALL_MCP_AUTH_PROVIDER=oidc
RECALL_OIDC_ISSUER=https://identity.example.com
RECALL_OIDC_JWKS_URI=https://identity.example.com/.well-known/jwks.json
```

Use `RECALL_MCP_AUTH_PROVIDER=descope` for the Descope preset and copy the exact
issuer and JWKS URL from the Descope MCP resource. The issuer must also appear
in `RECALL_AUTHORIZATION_SERVERS`. Startup fails closed for non-HTTPS,
credential-bearing, ambiguous, or mismatched URLs.

The public server exposes RFC 9728 protected-resource metadata at:

```text
https://recall.example.com/.well-known/oauth-protected-resource/mcp
```

Unauthenticated MCP requests receive that URL in `WWW-Authenticate`. The client
then discovers the authorization server, performs Authorization Code + PKCE,
and requests an access token whose audience is the exact
`RECALL_MCP_RESOURCE_URI`. Recall accepts bearer tokens only in the header; it
does not accept query-string tokens, cookies, trusted proxy identities, token
introspection fallbacks, or unsigned JWTs.

## Security invariants

- JWKS is owner-configured HTTPS, bounded to 256 KiB, cached for five minutes,
  and never follows redirects.
- Only RS256 keys with one exact `kid` are accepted; issuer, audience, time,
  and `read` scope are verified on every token.
- External subjects and access tokens are never stored in plaintext. Database
  bindings use a SHA-256 subject digest.
- A principal bound to several brains must use a brain-specific endpoint; the
  unscoped `/mcp` route fails closed when authority is ambiguous.
- Hosted and self-hosted deployments execute the same policy and audit code.

Protocol references:

- [MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization)
- [OAuth 2.0 Protected Resource Metadata](https://www.rfc-editor.org/rfc/rfc9728)
- [Descope MCP authorization](https://docs.descope.com/mcp)
