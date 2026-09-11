# Postgres CDC to ClickHouse

CDC from Postgres `order_events` into one PicoMQ stream per `region`, then one ClickHouse table.

Source: [`examples/connectors/postgres-cdc-clickhouse`](https://github.com/PicoMQ/picomq/tree/main/examples/connectors/postgres-cdc-clickhouse).

<div class="pico-diagram">
<svg viewBox="0 0 571 486" width="571" role="img" aria-label="Postgres CDC feeds the postgres source. The route template writes each row to orders.{region} on the PicoMQ node. The clickhouse sink reads orders.* over Kafka, unwraps the data field and inserts into one ClickHouse table.">
  <defs>
    <marker id="pca" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(-75 -10)">
    <rect x="95" y="30" width="139" height="56" class="box"/>
<text x="164" y="54" text-anchor="middle" class="label">Postgres :5432</text>
<text x="164" y="72" text-anchor="middle" class="sub">order_events</text>
    <rect x="257" y="30" width="182" height="56" class="box"/>
<text x="348" y="54" text-anchor="middle" class="label">postgres source</text>
<text x="348" y="72" text-anchor="middle" class="sub">cdc, slot picomq_orders</text>
    <rect x="463" y="30" width="162" height="56" class="box-accent"/>
<text x="544" y="54" text-anchor="middle" class="label">route</text>
<text x="544" y="72" text-anchor="middle" class="sub">orders.{data.region}</text>
    <rect x="183" y="160" width="99" height="36" class="box-accent"/>
<text x="233" y="183" text-anchor="middle" class="label">orders.eu</text>
    <rect x="303" y="160" width="99" height="36" class="box-accent"/>
<text x="352" y="183" text-anchor="middle" class="label">orders.na</text>
    <rect x="422" y="160" width="115" height="36" class="box-accent"/>
<text x="479" y="183" text-anchor="middle" class="label">orders.apac</text>
    <rect x="111" y="300" width="149" height="56" class="box"/>
<text x="186" y="324" text-anchor="middle" class="label">clickhouse sink</text>
<text x="186" y="342" text-anchor="middle" class="sub">pattern orders\..*</text>
    <rect x="284" y="300" width="146" height="56" class="box"/>
<text x="357" y="324" text-anchor="middle" class="label">unwrap_envelope</text>
<text x="357" y="342" text-anchor="middle" class="sub">field = data</text>
    <rect x="454" y="300" width="154" height="56" class="box"/>
<text x="532" y="324" text-anchor="middle" class="label">ClickHouse :8123</text>
<text x="532" y="342" text-anchor="middle" class="sub">order_events</text>
    <rect x="137" y="420" width="115" height="56" class="box"/>
<text x="195" y="444" text-anchor="middle" class="label">Postgres</text>
<text x="195" y="462" text-anchor="middle" class="sub">pico metadata</text>
    <rect x="276" y="420" width="128" height="56" class="box"/>
<text x="340" y="444" text-anchor="middle" class="label">RustFS :9000</text>
<text x="340" y="462" text-anchor="middle" class="sub">WAL and objects</text>
    <rect x="429" y="420" width="154" height="56" class="box"/>
<text x="506" y="444" text-anchor="middle" class="label">connectors :8081</text>
<text x="506" y="462" text-anchor="middle" class="sub">state volume</text>
    <rect x="169" y="134" width="381" height="76" class="edge-soft"/>
<text x="179" y="150" class="sub">pico</text>
    <path d="M233 58 L253 58" class="edge" marker-end="url(#pca)"/>
    <path d="M439 58 L459 58" class="edge" marker-end="url(#pca)"/>
    <path d="M544 86 L544 122" class="edge"/>
<path d="M233 122 L544 122" class="edge"/>
<path d="M233 122 L233 156" class="edge" marker-end="url(#pca)"/>
<path d="M352 122 L352 156" class="edge" marker-end="url(#pca)"/>
<path d="M479 122 L479 156" class="edge" marker-end="url(#pca)"/>
    <path d="M233 196 L233 272" class="edge"/>
<path d="M352 196 L352 272" class="edge"/>
<path d="M479 196 L479 272" class="edge"/>
<path d="M186 272 L479 272" class="edge"/>
<path d="M186 272 L186 296" class="edge" marker-end="url(#pca)"/>
    <path d="M260 328 L280 328" class="edge" marker-end="url(#pca)"/>
    <path d="M430 328 L450 328" class="edge" marker-end="url(#pca)"/>
    <text x="536" y="112" text-anchor="end" class="sub">one stream per region</text>
    <text x="178" y="266" text-anchor="end" class="sub">kafka :9092</text>
    <text x="360" y="408" text-anchor="middle" class="sub">also in the stack</text>
  </g>
</svg>
</div>

## Run

```bash
cd examples/connectors/postgres-cdc-clickhouse
docker compose up -d
./seed.sh
./verify.sh
```

| Service | Host | Auth |
| --- | --- | --- |
| Postgres (pgweb) | `http://localhost:8082` | `pico` / `pico` |
| PicoMQ | `http://localhost:9090` | |
| Connectors runtime | `http://localhost:8081` | |
| ClickHouse | `http://localhost:8123/play` | `pico` / `pico` |

## Connectors

Source, `postgres-source.toml`:

```toml
[[topics]]
topic = { strategy = "field", path = "data.region", template = "orders.{value}" }
create_topics = true

[plugin_config]
mode = "cdc"
tables = ["public.order_events"]
replication_slot = "picomq_orders"
capture_operations = ["INSERT", "UPDATE"]
```

Sink, `clickhouse-sink.toml`:

```toml
[[topics]]
pattern = "orders\\..*"

[transforms.unwrap_envelope]
enabled = true
field = "data"

[plugin_config]
table = "order_events"
insert_format = "json_each_row"
```

The ClickHouse table is not created by the sink. `sql/clickhouse.sql` creates it at container start.

| `table` | Result |
| --- | --- |
| `order_events` | One table, `region` as a column |
| `order_events_{topic_segment[-1]}` | `order_events_eu`, `order_events_na`, `order_events_apac` |

## Verify

| | Postgres | ClickHouse | PicoMQ |
| --- | --- | --- | --- |
| Counts | `SELECT region, count(*) FROM order_events GROUP BY region` | `SELECT region, uniqExact(id) FROM order_events GROUP BY region` | `pico ls --prefix /orders.` |
| EU rows | `SELECT * FROM order_events WHERE region = 'eu'` | `SELECT * FROM order_events WHERE region = 'eu'` | `pico read /orders.eu` |

## Resume

```bash
docker compose kill connectors
docker compose exec postgres psql -U pico -d example -c \
  "INSERT INTO order_events (order_id, region, merchant_id, \"type\", payload)
   VALUES ('ord-eu-resume', 'eu', 'lemongrass', 'placed', '{}'::jsonb);"
docker compose start connectors
./verify.sh
```

The row inserted while the runtime was down arrives after restart.
