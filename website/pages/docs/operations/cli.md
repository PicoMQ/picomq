# CLI

Everything ships in one binary. `pico` runs a node, acts as a client for both HTTP protocols, administers a cluster, and benchmarks it. Client commands print data to stdout and everything else to stderr, so output pipes cleanly.

## Connecting

Client commands take their connection from global flags, each with an environment variable equivalent: where to talk, which wire protocol, and the credential.

```bash
pico --endpoint http://node2.internal:4437 head /streams/orders
pico --protocol ds read /streams/orders
pico --http2 append /streams/orders
```

| Flag | Variable | Purpose |
| --- | --- | --- |
| `--endpoint` | `PICO_ENDPOINT` | The server base URL. Defaults to `http://127.0.0.1:4437` |
| `--protocol` | `PICO_PROTOCOL` | `pico` or `ds`, since the two differ on the wire. `bench` also takes `kafka` |
| `--http2` | `PICO_HTTP2` | Speaks HTTP/2 only, which multiplexes many concurrent appends over one connection |
| `--profile` | `PICO_PROFILE` | A saved profile to take the defaults from, see [Profiles](#profiles) |
| `--token` | `PICO_TOKEN` | The bearer credential |
| `--ca-certificate` | `PICO_CA_CERTIFICATE` | A PEM file of CA certificates to trust for `https` endpoints, on top of the system's, see [TLS](/docs/operations/tls#clients) |

The token rarely belongs on the command line. `pico auth login` stores it instead, in the OS keyring or a private `credentials.toml` next to the config when no keyring is available (`PICO_NO_KEYRING=1` forces the file, the right setting for CI), and every command attaches the stored credential automatically. An explicit flag or variable wins over storage. `pico auth status` shows where the credential lives, its id, and whether the endpoint accepts it, and `pico auth logout` removes it. Getting a token in the first place is covered in [Authentication](/docs/operations/auth).

## Profiles

Connection flags for a server you use often can be saved as a profile with `pico config`.

```bash
pico --endpoint http://node2.internal:4437 --protocol ds config set prod
pico config use prod
pico ls
```

`set` stores the global flags under a name, `get` and `ls` inspect them, `rm` deletes one, `use` picks the default, and `path` prints the file location, which `PICO_CONFIG` overrides. Flags always win over the profile, so a saved default can be overridden per invocation. Stored credentials are filed under the profile name too, so `pico auth login` against two clusters keeps their tokens apart.

## Stream commands

The verbs mirror the protocol. `create` is idempotent, `append` reads newline-delimited records from stdin, `read` catches up and returns, `tail` starts at the tail and `-f` keeps following.

```bash
pico create /streams/orders --content-type text/plain --partitions 3
seq 1 1000 | pico append /streams/orders --partition 0 --batch 100
pico read /streams/orders --partition 0
pico tail /streams/orders --partition 0 -f
pico head /streams/orders
pico ls --prefix /streams/
pico trim /streams/orders --partition 0 --seq 500
pico close /streams/orders
pico delete /streams/orders
```

`ls` and `trim` exist on the Pico protocol only, and the Durable Streams protocol appends one record per request, so `--batch` only applies to `pico`. On a topic with several partitions, `head`, `append`, `read`, `tail`, and `trim` take `--partition`.

## Topic settings

`create` takes a flag for every [topic setting](/docs/api#topic-settings), `describe` prints them, and `alter` changes the ones that can change, with `none` removing a setting.

```bash
pico create /streams/orders --partitions 3 --retention-ms 86400000 --schema orders.json --schema-validate
pico describe /streams/orders
pico alter /streams/orders --partitions 6 --retention-ms none --retention-bytes 1073741824
```

| `create` flag | `alter` flag |
| --- | --- |
| `--content-type` | |
| `--partitions` | `--partitions`, grow only |
| `--ttl` or `--expires-at` | |
| `--retention-ms`, `--retention-bytes` | The same, or `none` |
| `--schema`, `--schema-validate` | `--schema` or `none`, `--schema-validate true\|false` |
| `--closed` | |

[Schemas](/docs/schemas) are registered from a file or stdin, printed to stdout, and removed by name.

```bash
pico schema put orders.json --file orders.schema.json
pico schema get orders.json
pico schema rm orders.json
```

## Admin commands

Admin commands talk to a node's admin listener, not the stream endpoint. The target comes from `--admin-endpoint` or `PICO_ADMIN_ENDPOINT`, defaulting to `http://127.0.0.1:9090`. The credential comes from `--token` or `PICO_ADMIN_TOKEN`, then `PICO_TOKEN`, then the stored one. `--json` switches any command from formatted lines to raw JSON.

```bash
pico admin cluster
pico admin nodes
pico admin stream /streams/orders
pico admin transfer /streams/orders --to-node 2 --wait
pico admin set-slots 2 --slots 4
```

`cluster` prints the node's identity, counts, the applied index, the lease holder, and any pending transfers. `nodes` lists every registered node with its epoch, slots, stream counts, and whether it is fenced. `stream` shows one partition's owner, state, epoch, and offsets. `transfer` starts a move and with `--wait` polls until the partition settles on the target. Both take `--partition` for topics with several partitions. `set-slots` changes a node's placement weight.

## Benchmarking

`pico bench` writes and reads a temporary stream against any endpoint and reports throughput and latency percentiles.

```bash
pico bench --record-size 1024 --duration 15
```

It exercises the same client path as `append` and `read`, so the numbers reflect what an application would see, including protocol and redirect behavior. `--help` lists the load shape flags: in-flight requests, batch size, streams, partitions, connections, and a target rate.

`--protocol kafka` benchmarks the Kafka listener instead, through librdkafka. With a token it connects over `SASL_PLAINTEXT`, the token id as the username and the token as the password, so it needs a node with auth required and a token with the `kafka` audience, see [Kafka authentication](/docs/kafka#authentication). It cannot speak `SASL_SSL`, so the Kafka listener of a node running TLS is out of its reach.

## Serving

`pico serve` runs a node. Its flags configure the listeners, the metadata database, and the object store, and every one has a `PICO_*` environment variable. The flags are covered in [Configuration](/docs/operations/configuration) and full deployment layouts in [Deployment](/docs/operations/deployment/docker).

| The node stops because | It prints | Exit code |
| --- | --- | --- |
| `SIGTERM` or `Ctrl-C` | `shutting down` | `0` |
| It was fenced, or its WAL was taken over | `this node left the cluster, shutting down` | `1` |

A stopped node's streams move to live nodes either way, see [Failover](/docs/design/failover#when-a-node-stops). A supervisor should restart a node that exits with `1`: it rejoins at a new epoch.
