# CLI

Everything ships in one binary. `pico` runs a node, acts as a client for both HTTP protocols, administers a cluster, and benchmarks it. Client commands print data to stdout and everything else to stderr, so output pipes cleanly.

## Connecting

Client commands take their connection from global flags, each with an environment variable equivalent: where to talk, which wire protocol, and the credential.

```bash
pico --endpoint http://node2.internal:4437 head /streams/orders
pico --protocol ds read /streams/orders
pico --http2 append /streams/orders
```

`--endpoint` (`PICO_ENDPOINT`) is the server base URL, defaulting to `http://127.0.0.1:4437`. `--protocol` (`PICO_PROTOCOL`) selects `pico` or `ds`, since the two protocols differ on the wire. `kafka` is valid only for `serve`, because [Kafka clients](/docs/kafka) come from the Kafka ecosystem. `--http2` (`PICO_HTTP2`) speaks HTTP/2 over cleartext, which multiplexes many concurrent appends over one connection.

`--token` (`PICO_TOKEN`) is the bearer credential for a server running with auth required, and it rarely belongs on the command line. `pico auth login` stores it instead, in the OS keyring or a private `credentials.toml` next to the config when no keyring is available (`PICO_NO_KEYRING=1` forces the file, the right setting for CI), and every command attaches the stored credential automatically. An explicit flag or variable wins over storage. `pico auth status` shows where the credential lives, its id, and whether the endpoint accepts it, and `pico auth logout` removes it. Getting a token in the first place is covered in [Authentication](/docs/operations/auth).

## Profiles

Connection flags for a server you use often can be saved as a profile with `pico config`.

```bash
pico --endpoint http://node2.internal:4437 --protocol ds config set prod
pico config use prod
pico ls
```

`set` stores the global flags under a name, `get` and `ls` inspect them, `rm` deletes one, `use` picks the default, and `path` prints the file location. Flags always win over the profile, so a saved default can be overridden per invocation. Stored credentials are filed under the profile name too, so `pico auth login` against two clusters keeps their tokens apart.

## Stream commands

The verbs mirror the protocol. `create` is idempotent, `append` reads newline-delimited records from stdin, `read` catches up and returns, `tail` starts at the tail and `-f` keeps following.

```bash
pico create /streams/orders --content-type text/plain --ttl 86400
seq 1 1000 | pico append /streams/orders --batch 100
pico read /streams/orders
pico tail /streams/orders -f
pico head /streams/orders
pico ls --prefix /streams/
pico trim /streams/orders --seq 500
pico close /streams/orders
pico delete /streams/orders
```

`ls` and `trim` exist on the Pico protocol only, and the Durable Streams protocol appends one record per request, so `--batch` only applies to `pico`.

## Admin commands

Admin commands talk to a node's admin listener, not the stream endpoint. The target comes from `--admin-endpoint` or `PICO_ADMIN_ENDPOINT`, defaulting to `http://127.0.0.1:9090`, and `--json` switches any command from formatted lines to raw JSON.

```bash
pico admin cluster
pico admin nodes
pico admin stream /streams/orders
pico admin transfer /streams/orders --to-node 2 --wait
pico admin set-slots 2 --slots 4
```

`cluster` prints the node's identity, counts, the applied index, the lease holder, and any pending transfers. `nodes` lists every registered node with its slots and stream counts. `stream` shows one stream's owner, state, epoch, and offsets. `transfer` starts a move and with `--wait` polls until the stream settles on the target. `set-slots` changes a node's placement weight.

## Benchmarking

`pico bench` writes and reads a temporary stream against any endpoint and reports throughput and latency percentiles.

```bash
pico bench --record-size 1024 --duration 15
```

It exercises the same client path as `append` and `read`, so the numbers reflect what an application would see, including protocol and redirect behavior.

## Serving

`pico serve` runs a node. Its flags configure the listeners, the metadata database, and the object store, and every one has a `PICO_*` environment variable. The flags are covered in [Configuration](/docs/operations/configuration) and full deployment layouts in [Deployment](/docs/operations/deployment/docker).
