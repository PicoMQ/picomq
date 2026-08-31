# Authentication

How to turn auth on and run the token lifecycle. The model is in [Authorization](/docs/design/auth).

## Modes

`--auth` (`PICO_AUTH`) is `off` or `required`. With `required`, every request on both listeners needs a bearer token, except the health probes and CORS preflights. With `off`, nothing is checked and the node refuses to bind anything but loopback. Binding `0.0.0.0` means running with auth required, or opting out explicitly with `--insecure-allow-remote` (`PICO_INSECURE_ALLOW_REMOTE`) when something else guards the network, e.g. a private network or a proxy that authenticates.

## Bootstrap

The first token cannot come from the API, so the node seeds it at startup:

```bash
# wire form: BASE64URL(id).BASE64URL(32 random bytes)
TOKEN="$(printf 'ops/root' | basenc --base64url -w0 | tr -d '=').$(openssl rand 32 | basenc --base64url -w0 | tr -d '=')"

pico serve --auth required --auth-bootstrap-token "$TOKEN" ...
```

`--auth-bootstrap-token-file` reads the token from a file instead, which keeps it out of process listings. The stored record gets the root scope: every stream, every operation, every audience, no expiry. Bootstrap is idempotent. Every node can be given the same token and restart freely. A different token under the same id fails startup, so a live credential is never silently replaced. Use the root token to issue narrower tokens, not to run applications.

## Issuing tokens

Three endpoints on the admin listener:

| Method and path | What it does |
| --- | --- |
| `GET /admin/tokens` | List records visible to the caller, scopes and issuers, never secrets. |
| `POST /admin/tokens` | Issue a token narrowed from the caller's scope. `403` when it would widen. |
| `DELETE /admin/tokens/{id}` | Revoke. Effective on the next request, cluster-wide. |

```bash
curl -s -X POST http://127.0.0.1:9090/admin/tokens \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{
      "id": "svc/ingest",
      "scope": {
        "streams": [{ "prefix": "/logs/" }],
        "groups": { "stream": { "read": false, "write": true } },
        "audiences": ["pico"],
        "expiresAtMs": 1790000000000
      }
    }'
```

The response contains the wire token once. Only its hash is stored, so a lost secret means issuing a replacement.

In the scope JSON, `streams` and `tokens` are arrays of `{"exact": name}` or `{"prefix": p}` matchers, `groups` holds the three read/write pairs, `ops` names fine operations, `audiences` is any of `pico`, `durable_streams`, and `admin`, `autoPrefixStreams` turns on tenant prefixing, and `expiresAtMs` is unix milliseconds. Missing keys deny. Unknown keys are rejected.

The reserved id `anonymous` applies its scope to requests with no credential. Issue it read-only under `/public/` and those streams are readable by anyone while everything else stays closed. Revoke it and all access needs a token again. It cannot be given the `admin` audience.

## Client credentials

`pico auth login` stores a token per profile, in the OS keyring or in a private file with `PICO_NO_KEYRING=1`. `status` verifies it against the endpoint and `logout` removes it. An explicit `--token` or `PICO_TOKEN` wins over storage. See [CLI](/docs/operations/cli#connecting).

Programs using `picomq-client` set the token on the client config. The client follows `307`s itself and re-attaches the credential on every hop, because standard HTTP clients drop `Authorization` on cross-origin redirects and ownership redirects cross origins. The dashboard prompts for a token on rejection and holds it in session storage.

## Errors

`401` means no valid credential: absent, malformed, unknown, expired, or revoked. `403` means the credential is valid and the scope is insufficient. A `403` on appends can also be producer fencing, which is distinguishable by its `Producer-Epoch` header.

## Deployment notes

Auth controls who may call, not who can read the wire. Terminate TLS in front of the nodes before sending tokens over untrusted networks. The `harness/aio` compose files run with auth off for development; setting `PICO_AUTH=required` in `.env` turns it on with a known dev bootstrap token. `harness/byo` runs with auth required and refuses to start without a bootstrap token in `.env`. The Fly configs take it as an app secret.
