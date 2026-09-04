# First connector

This walk-through starts a node and the connectors runtime together, watches a source fan records out into topics that did not exist a moment earlier, and reads them back through a sink. It needs Docker and about ten minutes. Nothing is installed on the host.

<div class="pico-diagram">
<svg viewBox="0 30 720 170" width="720" role="img" aria-label="The random source produces into user-0 through user-3 on the pico node, and the stdout sink consumes them through a pattern subscription. Both plugins run in one connectors container.">
  <defs>
    <marker id="arrf1" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="40" width="680" height="150" fill="none" class="edge-soft" stroke-dasharray="4 4"/>
  <text x="360" y="60" text-anchor="middle" class="sub">docker compose: compose.lite.yml + compose.connectors.yml</text>
  <rect x="50" y="90" width="150" height="56" class="box"/>
  <text x="125" y="114" text-anchor="middle" class="label">random source</text>
  <text x="125" y="132" text-anchor="middle" class="sub">route by user_id</text>
  <rect x="280" y="80" width="160" height="76" class="box-accent"/>
  <text x="360" y="104" text-anchor="middle" class="label">pico</text>
  <text x="360" y="122" text-anchor="middle" class="sub">user-0 .. user-3</text>
  <text x="360" y="140" text-anchor="middle" class="sub">localhost:9092</text>
  <rect x="520" y="90" width="150" height="56" class="box"/>
  <text x="595" y="114" text-anchor="middle" class="label">stdout sink</text>
  <text x="595" y="132" text-anchor="middle" class="sub">pattern user-.*</text>
  <path d="M200 118 L272 118" class="edge" marker-end="url(#arrf1)"/>
  <path d="M440 118 L512 118" class="edge" marker-end="url(#arrf1)"/>
  <text x="236" y="108" text-anchor="middle" class="sub">produce</text>
  <text x="476" y="108" text-anchor="middle" class="sub">consume</text>
</svg>
</div>

## Start the stack

The `harness/aio` directory has the compose files for a self-contained node. `compose.connectors.yml` is an overlay that adds the runtime next to it, so it stacks on whichever base file you prefer. The lite one is enough here.

```bash
git clone https://github.com/picomq/picomq && cd picomq/harness/aio
docker compose -f compose.lite.yml -f compose.connectors.yml up --build
```

The first build compiles the runtime and its plugins, which takes a while. Once both containers are up the runtime loads two connector definitions from `harness/aio/connectors`, and the stdout sink starts printing batches within a couple of seconds.

```text
connectors-1  | Loading connector configuration from: /etc/picomq-connectors/connectors/random-source.toml
connectors-1  | Loading connector configuration from: /etc/picomq-connectors/connectors/stdout-sink.toml
connectors-1  | Resolved plugin path: /usr/local/lib/libpicomq_connector_random_source.so (found in /usr/local/lib)
connectors-1  | Resolved plugin path: /usr/local/lib/libpicomq_connector_stdout_sink.so (found in /usr/local/lib)
connectors-1  | Stdout sink with ID: 1 received: 7 messages, schema: json, topic: user-2, partition: 0, offset: 0, invocation: 1
connectors-1  | Stdout sink with ID: 1 received: 5 messages, schema: json, topic: user-0, partition: 0, offset: 0, invocation: 2
```

## The source

The source is the `random` plugin. It generates JSON records with a `sequence` number and a `user_id` drawn from a small pool.

```toml
type = "source"
key = "random"
enabled = true
version = 0
name = "Random source"
path = "libpicomq_connector_random_source"

[[topics]]
topic = { strategy = "field", path = "user_id", template = "{value}" }
schema = "json"
batch_length = 100
linger_time = "5ms"
create_topics = true

[plugin_config]
interval = "1s"
messages_range = [5, 20]
payload_size = 64
user_pool = 4
```

The interesting line is `topic`. Instead of a name it carries a rule, and each second the runtime does the following with the batch the plugin hands over.

1. Reads `user_id` from each record and uses it as the topic name.
2. Creates `user-0` through `user-3` the first time each is seen, because `create_topics` is on.
3. Produces every record and waits for the broker to acknowledge all of them.
4. Tells the plugin the batch is done, so it can advance its sequence counter.

From the node's side these are ordinary streams, created by a Kafka client. They show up in the dashboard and over HTTP like any other.

## The sink

The sink is the `stdout` plugin. It does not list topics. It subscribes to a pattern, and the runtime re-checks the pattern against the broker every couple of seconds, so the four topics the source created were picked up without anyone naming them.

```toml
type = "sink"
key = "stdout"
enabled = true
version = 0
name = "Stdout sink"
path = "libpicomq_connector_stdout_sink"

[[topics]]
pattern = "user-.*"
schema = "json"
batch_length = 100
poll_interval = "100ms"

[plugin_config]
print_payload = true
```

A source that names topics from data and a sink that follows a pattern is the shape most PicoMQ connector deployments take. [Routing and templating](/docs/connectors/routing) covers the other strategies.

## Look around

The runtime serves an HTTP API on port 8081, and the node sees the topics as streams.

```bash
curl -s localhost:8081/sinks | jq              # running sinks and their status
curl -s localhost:8081/sources/random | jq      # one source in detail
curl -s localhost:8081/stats | jq               # throughput counters
curl -s localhost:4437/user-1 | head            # the topic, read as a stream over HTTP
```

## Break it

Kill the runtime container and start it again.

```bash
docker compose -f compose.lite.yml -f compose.connectors.yml kill connectors
docker compose -f compose.lite.yml -f compose.connectors.yml up connectors
```

Two things happen on the way back up.

- The source resumes its sequence numbers where it left off. Its state, the count of messages produced, was checkpointed to the `connectors-state` volume after every acknowledged batch.
- The sink resumes at the offset it last committed. The log shows no gap in the sequence numbers it prints.

Kill it during a batch instead, with `kill -9` on the process rather than a stop, and the sink may print a handful of sequence numbers twice. That is the at-least-once guarantee doing what it says. [Delivery guarantees](/docs/connectors/delivery) explains exactly which window is exposed and how each sink absorbs it.

## Your own connector

Drop another file into `harness/aio/connectors` and restart the runtime. This one lands every `user-*` topic in its own Postgres table, given a Postgres you have to hand. The [Postgres sink](/docs/connectors/sinks/postgres) page has every option.

```toml
type = "sink"
key = "users_pg"
enabled = true
version = 0
name = "Users to Postgres"
path = "libpicomq_connector_postgres_sink"

[[topics]]
pattern = "user-.*"
schema = "json"
batch_length = 500

[plugin_config]
connection_string = "postgres://user:pass@host:5432/app"
target_table = "events_{topic}"
auto_create_table = true
```

From here, [Sources](/docs/connectors/sources) and [Sinks](/docs/connectors/sinks) explain what the runtime did on each side, and [Operations](/docs/operations/connectors) covers running it outside the harness.
