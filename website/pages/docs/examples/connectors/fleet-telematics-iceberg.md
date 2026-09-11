# Fleet telematics to Iceberg

Location pings from two fleets into one PicoMQ stream per fleet, then one Iceberg table.

Source: [`examples/connectors/fleet-telematics-iceberg`](https://github.com/PicoMQ/picomq/tree/main/examples/connectors/fleet-telematics-iceberg).

<div class="pico-diagram">
<svg viewBox="0 0 690 512" width="690" role="img" aria-label="seed.sh appends pings to fleets.a and fleets.b. The iceberg sink reads fleets.* and writes Parquet to the RustFS warehouse, committing snapshots to the Iceberg REST catalog. createtable registers the table first. DuckDB attaches the catalog to query.">
  <defs>
    <marker id="fta" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(20 6)">
    <rect x="0" y="40" width="175" height="56" class="box"/>
<text x="88" y="64" text-anchor="middle" class="label">seed.sh</text>
<text x="88" y="82" text-anchor="middle" class="sub">pico append --batch 20</text>
    <rect x="219" y="40" width="130" height="36" class="box-accent"/>
<text x="284" y="63" text-anchor="middle" class="label">fleets.a</text>
    <rect x="219" y="88" width="130" height="36" class="box-accent"/>
<text x="284" y="111" text-anchor="middle" class="label">fleets.b</text>
    <rect x="393" y="40" width="149" height="56" class="box"/>
<text x="468" y="64" text-anchor="middle" class="label">iceberg sink</text>
<text x="468" y="82" text-anchor="middle" class="sub">pattern fleets\..*</text>
    <rect x="285" y="200" width="170" height="56" class="box"/>
<text x="370" y="224" text-anchor="middle" class="label">iceberg-rest :8181</text>
<text x="370" y="242" text-anchor="middle" class="sub">catalog</text>
    <rect x="495" y="200" width="155" height="56" class="box"/>
<text x="573" y="224" text-anchor="middle" class="label">RustFS :9000</text>
<text x="573" y="242" text-anchor="middle" class="sub">s3://lake/warehouse</text>
    <rect x="110" y="200" width="135" height="56" class="box"/>
<text x="177" y="224" text-anchor="middle" class="label">createtable</text>
<text x="177" y="242" text-anchor="middle" class="sub">namespace, table</text>
    <rect x="285" y="320" width="366" height="56" class="box"/>
<text x="468" y="344" text-anchor="middle" class="label">duckdb</text>
<text x="468" y="362" text-anchor="middle" class="sub">ATTACH iceberg, httpfs</text>
    <rect x="15" y="430" width="139" height="56" class="box"/>
<text x="85" y="454" text-anchor="middle" class="label">Postgres :5432</text>
<text x="85" y="472" text-anchor="middle" class="sub">pico metadata</text>
    <rect x="178" y="430" width="170" height="56" class="box"/>
<text x="263" y="454" text-anchor="middle" class="label">RustFS s3://picomq</text>
<text x="263" y="472" text-anchor="middle" class="sub">pico WAL and objects</text>
    <rect x="372" y="430" width="154" height="56" class="box"/>
<text x="449" y="454" text-anchor="middle" class="label">connectors :8081</text>
<text x="449" y="472" text-anchor="middle" class="sub">state volume</text>
    <rect x="205" y="14" width="158" height="124" class="edge-soft"/>
<text x="215" y="30" class="sub">pico :9092</text>
    <path d="M175 68 L201 68" class="edge" marker-end="url(#fta)"/>
    <path d="M363 68 L389 68" class="edge" marker-end="url(#fta)"/>
    <path d="M468 96 L468 160" class="edge"/>
<path d="M370 160 L573 160" class="edge"/>
<path d="M370 160 L370 196" class="edge" marker-end="url(#fta)"/>
<path d="M573 160 L573 196" class="edge" marker-end="url(#fta)"/>
    <path d="M245 228 L281 228" class="edge" marker-end="url(#fta)"/>
    <path d="M370 320 L370 260" class="edge-soft" marker-end="url(#fta)"/>
    <path d="M573 320 L573 260" class="edge-soft" marker-end="url(#fta)"/>
    <text x="362" y="190" text-anchor="end" class="sub">snapshot</text>
    <text x="581" y="190" text-anchor="start" class="sub">parquet</text>
    <text x="468" y="306" text-anchor="middle" class="sub">SELECT</text>
    <text x="271" y="418" text-anchor="middle" class="sub">also in the stack</text>
  </g>
</svg>
</div>

## Run

```bash
cd examples/connectors/fleet-telematics-iceberg
docker compose up -d --build
./seed.sh
./verify.sh
```

| Service | Host |
| --- | --- |
| PicoMQ | `http://localhost:9090` |
| Connectors runtime | `http://localhost:8081` |
| Iceberg REST catalog | `http://localhost:8181` |
| RustFS | `http://localhost:9001` |
| DuckDB | `docker compose exec duckdb duckdb -init /opt/duckdb.sql` |

`--build` compiles the Iceberg sink image. It is not in the default connectors image.

## Sink

`iceberg-sink.toml`:

```toml
[[topics]]
pattern = "fleets\\..*"

[plugin_config]
tables = ["analytics.locations"]
catalog_type = "rest"
uri = "http://iceberg-rest:8181"
warehouse = "s3://lake/warehouse/"
store_url = "http://rustfs:9000"
store_path_style_access = true
```

The table is not created by the sink. `createtable` in `compose.yml` posts `sql/namespace.json` and `sql/table.json` to the catalog first.

| `tables` | Result |
| --- | --- |
| `analytics.locations` | One table, `fleet_id` as a column |
| `analytics.locations_{topic_segment[-1]}` | `analytics.locations_a`, `analytics.locations_b` |

## Verify

| | PicoMQ | Iceberg (DuckDB) |
| --- | --- | --- |
| Fleets | `pico ls --prefix /fleets.` | `SELECT fleet_id, count(*) FROM lake.analytics.locations GROUP BY 1` |
| Fleet A | `pico read /fleets.a` | `SELECT * FROM lake.analytics.locations WHERE fleet_id = 'a'` |

## Resume

```bash
docker compose kill connectors
printf '%s\n' '{"id":"a-resume","fleet_id":"a","vehicle_id":"van-1","lat":51.51,"lon":-0.12,"ts":"2026-09-05T21:00:00Z"}' \
  | docker compose exec -T pico pico append /fleets.a --batch 1
docker compose start connectors
./verify.sh
```
