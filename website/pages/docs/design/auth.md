# Authorization

PicoMQ authenticates requests with opaque bearer tokens and authorizes them with scopes stored next to the tokens in the metadata state. There are no signed claims and no keys to distribute. The token is a random secret and the server stores only a SHA-256 hash of it, so reading the metadata, a backup, or a log never yields a usable credential. Setup and operation are covered in [Authentication](/docs/operations/auth).

## Tokens

On the wire a token is `BASE64URL(id).BASE64URL(secret)`. The id is public. The secret appears once in the issuing response and is never stored. Records live in the replicated metadata KV under the reserved `auth/token/` prefix, apart from the topic keys described in [Topics and streams](/docs/design/streams#what-is-stored). Issuance and revocation replicate like any other metadata command.

## Scopes

A scope answers three independent questions.

- **Which resources.** Three matcher sets of exact names and prefixes: streams, token ids, and consumer groups.
- **Which operations.** Read and write flags for each access category, plus nineteen single operations for tokens that need one verb, such as an append-only producer.
- **Which listeners.** The audiences `pico`, `durable_streams`, `kafka`, and `admin`.

A scope may also have an absolute expiry. An expired token fails on its next use, and a sweep on the maintenance-lease holder deletes the record.

| Access | Matched against | Read | Write |
| --- | --- | --- | --- |
| `stream` | streams | `read`, `head`, `list` | `create`, `append`, `trim`, `close`, `delete` |
| `tokens` | token ids | `list_tokens` | `issue_token`, `revoke_token` |
| `group` | consumer groups | `consume`, `describe_group` | `delete_group` |
| `admin` | nothing, they act on the whole cluster | `cluster_read`, `node_read`, `stream_inspect` | `transfer_stream`, `update_node_slots` |

Every operation is checked against the set its row names, so `consume` on the group `billing-eu` needs a group matcher that covers `billing-eu`.

- **Describing.** Any right on a stream grants `head` on it, and any right on a group grants `describe_group` on it. A write-only producer can see what it writes to, and a group's deleter can see the group.
- **Groups.** Reading a topic does not make a token a member of every group on it. Taking part in a group as a member, committing its offsets, or leaving it needs `consume` on that group, as Kafka checks `Read` on the group as well as the topic.
- **Names.** Stream names in matchers are canonical, with `/` and `.` as the same separator, so one prefix covers a stream under every protocol's spelling, see [Topics and streams](/docs/design/streams#names). Group names match exactly as clients send them.

## Narrowing

Any token with the `issue_token` operation can create new tokens, but only narrower ones.

- **Resources.** Each of the child's three matcher sets must fall within the issuer's.
- **Operations.** An access flag on the child needs the same flag on the issuer. A single operation on the child is covered by an issuer flag or by the same operation. Single operations on the issuer never cover a child flag, because a flag also covers operations added later.
- **Audiences and expiry.** The child's audiences must be ones the issuer holds, and its expiry must not be later.
- **Ids.** The issuer's token-id matcher limits which ids it may create, so a token allowed to issue under `svc/` cannot create `ops/root`.

A request that would widen is rejected, nothing is silently clamped. Scopes with no operations or no audiences are rejected at issue time.

## Auto-prefixing

A scope with `autoPrefix` holds exactly one stream prefix and one group prefix, and every stream and group name a client sends is relative to them. For a token with the stream prefix `/tenants/a/` and the group prefix `tenants/a/`:

| Client sends | Stored as | Read back as |
| --- | --- | --- |
| stream `orders` | `/tenants/a/orders` | `orders` |
| group `workers` | `tenants/a/workers` | `workers` |

Names are always relative. Naming another tenant's full path resolves inside the caller's own prefix, so there is no way out, and two tenants using the same group name get two separate groups. Lists show only the caller's own subtree. Ownership routing uses the resolved name, and redirects preserve the client's original path.

## The gate

Enforcement runs once the request body is read and before routing. The gate works out what the request will do from the parsed request, the same way the handler will:

- A `POST` with a `Pico-Trim-Seq` header needs `trim`. A `POST` that closes the stream needs `close`, and `append` too when it carries records.
- A Durable Streams `PUT` with a body needs `create` and `append`.
- A list needs `list` on the requested prefix, and returns only the topics the token may read.
- Consumer group routes check the group and the topics separately. The coordinator checks the group's right from the [Scopes](#scopes) table on the group itself: `consume` to take part, `describe_group` to describe it or fetch its offsets, `delete_group` to delete it or its offsets. A join or a commit also needs `read` on every topic it names, and a list returns only the groups the token may describe.
- A method the protocol does not serve is refused with `405` before any of this.
- On Kafka the SASL exchange fixes the principal once per connection, and every request then passes the same gate against the record as it currently stands. The frontend translates each request kind into an operation and each refusal into Kafka's authorization codes, the decisions are the scope's, see [Kafka](/docs/kafka#authentication).

A streaming read that fails auth gets an immediate plain response, never a held connection. CORS preflights pass without a credential. A request with no credential gets the scope of the reserved `anonymous` token when one is stored, which makes public access a replicated grant instead of a server mode. Denials against the anonymous scope return `401`, because the fix is to present a credential.

Rejections use each protocol's own error format, JSON on Pico and plain text on Durable Streams, with no new headers. `401` means no valid credential and `403` means insufficient scope. Producer fencing also uses `403` and stays distinguishable by its `Producer-Epoch` header.

## Revocation

A revoke deletes the record through a conditional metadata command that includes the expected verifier, so it cannot race a reissue of the same id. The delete is acknowledged only after it applies, and the authorizer reads the applied state, so a revoked token is dead on the next request. There is no TTL to wait out and no cache window. On HTTP that request gets `401`, and on Kafka the connection closes, so the client reconnects and fails the SASL exchange rather than waiting on a timeout. Expiry and replacement under the same id take effect the same way.

## The control plane

Token issue, list, and revoke are admin endpoints. The stream listeners implement fixed protocols, one of them an external specification with no token management, and the admin listener is already the surface with separate exposure rules.

## Deliberately excluded

These are left out for now:

- Per-token rate and byte quotas.
- OIDC or mTLS federation.
- CIDR and time-of-day binding.
- SCRAM over Kafka, which needs salted keys stored at issue time. PLAIN works from the hash alone.
