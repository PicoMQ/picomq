# Authorization

PicoMQ authenticates requests with opaque bearer tokens and authorizes them with scopes stored next to the tokens in the metadata state. There are no signed claims and no keys to distribute. The token is a random secret and the server stores only a SHA-256 hash of it, so reading the metadata, a backup, or a log never yields a usable credential. Setup and operation are covered in [Authentication](/docs/operations/auth).

## Tokens

On the wire a token is `BASE64URL(id).BASE64URL(secret)`. The id is public. The secret appears once in the issuing response and is never stored. Records live in the replicated metadata KV under the reserved `auth/token/` prefix. Stream registry keys always start with `/` and the auth prefix does not, so the two can never collide. Issuance and revocation replicate like any other metadata command.

## Scopes

A scope answers three independent questions. Which resources: matcher sets of exact names and prefixes, one set for streams and one for token ids. Which operations: three coarse read/write groups (`stream`, `tokens`, `admin`) and sixteen fine operations for tokens that need a single verb, such as an append-only producer. Which listeners: the audiences `pico`, `durable_streams`, and `admin`. A scope may also have an absolute expiry. An expired token fails on its next use, and a sweep on the maintenance-lease holder deletes the record.

## Narrowing

Any token with the `IssueToken` operation can create new tokens, but only narrower ones. The child's resources must fall within the issuer's matchers, its operations and audiences must be ones the issuer holds, and its expiry must not be later. A request that would widen is rejected, nothing is silently clamped. A parent group covers a child's fine operations, but parent fine operations never cover a child group, because a group also includes operations added later. The issuer's token-id matcher limits which ids it may create, so a token allowed to issue under `svc/` cannot create `ops/root`. Scopes with no operations or no audiences are rejected at issue time.

## Auto-prefixing

A scope with `autoPrefixStreams` holds exactly one stream prefix, and client stream names are relative to it. A token prefixed at `/tenants/a/` creates `orders` as `/tenants/a/orders`, reads it back as `orders`, and lists only its own subtree. Naming another tenant's full path resolves inside the caller's own prefix, so there is no way out. Ownership routing uses the resolved name, and redirects preserve the client's original path.

## The gate

Enforcement runs at the top of dispatch, before routing and body buffering. A streaming read that fails auth gets an immediate plain response, never a held connection. CORS preflights pass without a credential. A request with no credential gets the scope of the reserved `anonymous` token when one is stored, which makes public access a replicated grant instead of a server mode. Denials against the anonymous scope return `401`, because the fix is to present a credential.

Rejections use each protocol's own error format, JSON on Pico and plain text on Durable Streams, with no new headers. `401` means no valid credential and `403` means insufficient scope. Producer fencing also uses `403` and stays distinguishable by its `Producer-Epoch` header.

## Revocation

A revoke deletes the record through a conditional metadata command that includes the expected verifier, so it cannot race a reissue of the same id. The delete is acknowledged only after it applies, and the authorizer reads the applied state, so a revoked token is dead on the next request. There is no TTL to wait out and no cache window.

## The control plane

Token issue, list, and revoke are admin endpoints. The stream listeners implement fixed protocols, one of them an external specification with no token management, and the admin listener is already the surface with separate exposure rules.

## Deliberately excluded

The scope encoding is versioned and additive, so these can be added later without a format break: per-token rate and byte quotas, OIDC or mTLS federation, CIDR and time-of-day binding. TLS belongs to the proxy in front.
