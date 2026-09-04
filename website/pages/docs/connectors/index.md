# Connectors

Connectors move records between PicoMQ and the systems around it. A source reads from somewhere else and produces into topics. A sink consumes topics and writes them out.

Both run inside `pico-connectors`, a process separate from the node. It speaks only the Kafka protocol to PicoMQ and needs nothing but a bootstrap address. It deploys, scales and restarts on its own schedule, and the cluster never knows it is there.

<div class="pico-diagram">
<svg viewBox="0 20 720 260" width="720" role="img" aria-label="External systems on the left feed source plugins inside the connectors runtime, which produce into PicoMQ topics over Kafka. Sink plugins in the same runtime consume those topics and write to external systems on the right.">
  <defs>
    <marker id="arrc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="60" width="130" height="56" class="box"/>
  <text x="85" y="84" text-anchor="middle" class="label">Postgres</text>
  <text x="85" y="102" text-anchor="middle" class="sub">replication slot</text>
  <rect x="20" y="150" width="130" height="56" class="box"/>
  <text x="85" y="174" text-anchor="middle" class="label">Elasticsearch</text>
  <text x="85" y="192" text-anchor="middle" class="sub">index</text>
  <rect x="190" y="40" width="340" height="200" fill="none" class="edge-soft" stroke-dasharray="4 4"/>
  <text x="360" y="60" text-anchor="middle" class="sub">pico-connectors</text>
  <rect x="210" y="80" width="120" height="56" class="box"/>
  <text x="270" y="104" text-anchor="middle" class="label">source plugin</text>
  <text x="270" y="122" text-anchor="middle" class="sub">.so, poll + ack</text>
  <rect x="390" y="80" width="120" height="56" class="box"/>
  <text x="450" y="104" text-anchor="middle" class="label">sink plugin</text>
  <text x="450" y="122" text-anchor="middle" class="sub">.so, consume</text>
  <rect x="210" y="160" width="300" height="56" class="box-accent"/>
  <text x="360" y="184" text-anchor="middle" class="label">Kafka client</text>
  <text x="360" y="202" text-anchor="middle" class="sub">produce, fetch, consumer groups, admin</text>
  <rect x="570" y="60" width="130" height="56" class="box"/>
  <text x="635" y="84" text-anchor="middle" class="label">ClickHouse</text>
  <text x="635" y="102" text-anchor="middle" class="sub">table per topic</text>
  <rect x="570" y="150" width="130" height="56" class="box"/>
  <text x="635" y="174" text-anchor="middle" class="label">S3</text>
  <text x="635" y="192" text-anchor="middle" class="sub">parquet files</text>
  <path d="M150 88 L202 104" class="edge" marker-end="url(#arrc)"/>
  <path d="M150 178 L202 112" class="edge" marker-end="url(#arrc)"/>
  <path d="M270 136 L270 152" class="edge" marker-end="url(#arrc)"/>
  <path d="M450 152 L450 136" class="edge" marker-end="url(#arrc)"/>
  <path d="M510 104 L562 88" class="edge" marker-end="url(#arrc)"/>
  <path d="M510 112 L562 178" class="edge" marker-end="url(#arrc)"/>
  <path d="M360 216 L360 262" class="edge" marker-start="url(#arrc)" marker-end="url(#arrc)"/>
  <text x="380" y="262" class="sub">PicoMQ node, Kafka listener :9092</text>
</svg>
</div>

## Plugins, not a monolith

The runtime contains no connector code. Each connector is a shared library, a `.so` on Linux, loaded at startup from a path in its definition. The runtime drives it through a small C ABI: open with a config blob, exchange batches, close.

The two sides own different things.

| Runtime owns | Plugin owns |
| --- | --- |
| Consumer groups and offsets | Reading the external system |
| The producer and topic creation | Writing the external system |
| Checkpoints for sources | What its checkpoint means |
| Retries and backoff | Its own connection handling |
| Decoding, transforms, routing | Its configuration schema |
| The HTTP API and metrics | Nothing about PicoMQ |

That split has consequences worth knowing up front.

- An installation carries exactly the connectors it uses. The image ships the light plugins, and a heavy one is a single file dropped into `/usr/local/lib`.
- A connector written outside this repository installs the same way as one inside it. The ABI is the contract, not the workspace.
- A plugin that panics takes the runtime down with it. The runtime is meant to be supervised and restarted, and the checkpointing in [Delivery guarantees](/docs/connectors/delivery) is designed around that.

## Topics are the unit

PicoMQ encourages many small streams instead of a few wide ones, and connectors are built for that.

- A source can produce straight into a topic per user, per tenant or per hash bucket by naming the topic from a field of each record.
- A sink can subscribe to a pattern and follow topics into existence as they appear.
- A sink can resolve a table or index per topic from a template, so the fan-out on the way in becomes a fan-out on the way out.

<div class="pico-diagram">
<svg viewBox="0 20 720 210" width="720" role="img" aria-label="One source stream fans out into per-user topics, a pattern-subscribed sink gathers them, and a destination template lands each in its own table.">
  <defs>
    <marker id="arrt" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="102" width="110" height="50" class="box"/>
  <text x="75" y="123" text-anchor="middle" class="label">source</text>
  <text x="75" y="141" text-anchor="middle" class="sub">users table</text>
  <rect x="200" y="50" width="110" height="36" class="box-accent"/>
  <text x="255" y="73" text-anchor="middle" class="label">user-17</text>
  <rect x="200" y="109" width="110" height="36" class="box-accent"/>
  <text x="255" y="132" text-anchor="middle" class="label">user-42</text>
  <rect x="200" y="168" width="110" height="36" class="box-accent"/>
  <text x="255" y="191" text-anchor="middle" class="label">user-91</text>
  <rect x="380" y="102" width="110" height="50" class="box"/>
  <text x="435" y="123" text-anchor="middle" class="label">sink</text>
  <text x="435" y="141" text-anchor="middle" class="sub">user-.*</text>
  <rect x="560" y="50" width="140" height="36" class="box"/>
  <text x="630" y="73" text-anchor="middle" class="label">events_user_17</text>
  <rect x="560" y="109" width="140" height="36" class="box"/>
  <text x="630" y="132" text-anchor="middle" class="label">events_user_42</text>
  <rect x="560" y="168" width="140" height="36" class="box"/>
  <text x="630" y="191" text-anchor="middle" class="label">events_user_91</text>
  <path d="M130 120 L192 70" class="edge" marker-end="url(#arrt)"/>
  <path d="M130 127 L192 127" class="edge" marker-end="url(#arrt)"/>
  <path d="M130 134 L192 184" class="edge" marker-end="url(#arrt)"/>
  <path d="M310 70 L372 120" class="edge" marker-end="url(#arrt)"/>
  <path d="M310 127 L372 127" class="edge" marker-end="url(#arrt)"/>
  <path d="M310 184 L372 134" class="edge" marker-end="url(#arrt)"/>
  <path d="M490 120 L552 70" class="edge" marker-end="url(#arrt)"/>
  <path d="M490 127 L552 127" class="edge" marker-end="url(#arrt)"/>
  <path d="M490 134 L552 184" class="edge" marker-end="url(#arrt)"/>
  <text x="165" y="40" text-anchor="middle" class="sub">route by field</text>
  <text x="525" y="40" text-anchor="middle" class="sub">template per topic</text>
</svg>
</div>

[Routing and templating](/docs/connectors/routing) covers this in full. It is the main thing that makes these connectors different from the Kafka Connect model they otherwise resemble.

## What you get, and what you do not

Delivery is at-least-once in both directions. Sinks commit their consumer offset only after the plugin confirms the write. Sources advance their cursor only after every record of a batch is acknowledged by the broker. After a crash a record may be seen twice, never zero times, and most sinks upsert on a deterministic id so the duplicate is invisible.

Two things are not offered, and both come from PicoMQ itself rather than the connectors.

- No exactly-once. PicoMQ does not support Kafka transactions, and the sources have no equivalent on their side, so the runtime does not pretend.
- One partition per topic. This is how PicoMQ works, and it simplifies the connectors considerably, since a topic is a single ordered stream and a sink never reasons about partition assignment.

Both hold for any Kafka client against PicoMQ, not only for connectors. The [Kafka protocol](/docs/kafka) page has the details.

## Where to go next

| Page | Read it when |
| --- | --- |
| [First connector](/docs/connectors/first-connector) | You want records flowing in ten minutes |
| [Sources](/docs/connectors/sources) and [Sinks](/docs/connectors/sinks) | You want to know what the runtime does on each side |
| [Routing and templating](/docs/connectors/routing) | You are designing the topic layout |
| [Delivery guarantees](/docs/connectors/delivery) | You are about to trust production data to it |
| Catalog | You need every option of a specific connector |
| [Writing a plugin](/docs/connectors/plugin-sdk) | The system you need is not in the catalog |
| [Operations](/docs/operations/connectors) | You are deploying and running the runtime |
