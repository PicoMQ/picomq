# Flink Dynamic Kafka

Flink discovers PicoMQ streams by prefix, consumes them over the Kafka listener, and writes one file bucket per topic. Streams added or removed while the job runs are picked up on the next poll.

Source: [`examples/connectors/flink-dynamic-kafka`](https://github.com/PicoMQ/picomq/tree/main/examples/connectors/flink-dynamic-kafka).

<div class="pico-diagram">
<svg viewBox="0 0 998 386" width="998" role="img" aria-label="Flink polls the PicoMQ HTTP listener for streams with the fleets. prefix, consumes them as Kafka topics through DynamicKafkaSource, and writes one file bucket per topic. Streams created or deleted by the scripts are picked up on the next poll.">
  <defs>
    <marker id="fda" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(34 6)">
    <rect x="-14" y="14" width="322" height="244" class="edge-soft"/>
<text x="-4" y="30" class="sub">pico</text>
    <rect x="390" y="14" width="479" height="240" class="edge-soft"/>
<text x="400" y="30" class="sub">flink jobmanager + taskmanager</text>
    <rect x="0" y="40" width="294" height="56" class="box"/>
<text x="147" y="64" text-anchor="middle" class="label">http :4437</text>
<text x="147" y="82" text-anchor="middle" class="sub">GET /?prefix=/fleets.</text>
    <rect x="0" y="112" width="294" height="56" class="box"/>
<text x="147" y="136" text-anchor="middle" class="label">kafka :9092</text>
<text x="147" y="154" text-anchor="middle" class="sub">fetch</text>
    <rect x="0" y="208" width="91" height="36" class="box-accent"/>
<text x="46" y="231" text-anchor="middle" class="label">fleets.a</text>
    <rect x="101" y="208" width="91" height="36" class="box-accent"/>
<text x="147" y="231" text-anchor="middle" class="label">fleets.b</text>
    <rect x="202" y="208" width="91" height="36" class="box"/>
<text x="248" y="231" text-anchor="middle" class="label">fleets.c</text>
    <rect x="404" y="40" width="218" height="56" class="box"/>
<text x="512" y="64" text-anchor="middle" class="label">HttpKafkaMetadataService</text>
<text x="512" y="82" text-anchor="middle" class="sub">poll prefix, topics</text>
    <rect x="404" y="112" width="218" height="56" class="box"/>
<text x="512" y="136" text-anchor="middle" class="label">DynamicKafkaSource</text>
<text x="512" y="154" text-anchor="middle" class="sub">bootstrap pico:9092</text>
    <rect x="645" y="112" width="210" height="56" class="box"/>
<text x="750" y="136" text-anchor="middle" class="label">TopicRecordDeserializer</text>
<text x="750" y="154" text-anchor="middle" class="sub">topic, value</text>
    <rect x="645" y="184" width="210" height="56" class="box"/>
<text x="750" y="208" text-anchor="middle" class="label">FileSink</text>
<text x="750" y="226" text-anchor="middle" class="sub">bucket by topic, 2s checkpoint</text>
    <rect x="556" y="304" width="123" height="36" class="box"/>
<text x="617" y="327" text-anchor="middle" class="label">out/fleets.a</text>
    <rect x="689" y="304" width="123" height="36" class="box"/>
<text x="750" y="327" text-anchor="middle" class="label">out/fleets.b</text>
    <rect x="821" y="304" width="123" height="36" class="box"/>
<text x="883" y="327" text-anchor="middle" class="label">out/fleets.c</text>
    <rect x="-14" y="304" width="218" height="56" class="box"/>
<text x="95" y="328" text-anchor="middle" class="label">seed.sh add.sh remove.sh</text>
<text x="95" y="346" text-anchor="middle" class="sub">pico append, pico delete</text>
    <path d="M404 68 L298 68" class="edge" marker-end="url(#fda)"/>
    <path d="M404 140 L298 140" class="edge" marker-end="url(#fda)"/>
    <path d="M512 96 L512 108" class="edge" marker-end="url(#fda)"/>
    <path d="M621 140 L641 140" class="edge" marker-end="url(#fda)"/>
    <path d="M750 168 L750 180" class="edge" marker-end="url(#fda)"/>
    <path d="M750 240 L750 278" class="edge"/>
<path d="M617 278 L883 278" class="edge"/>
<path d="M617 278 L617 300" class="edge" marker-end="url(#fda)"/>
<path d="M750 278 L750 300" class="edge" marker-end="url(#fda)"/>
<path d="M883 278 L883 300" class="edge" marker-end="url(#fda)"/>
    <path d="M46 304 L46 262" class="edge" marker-end="url(#fda)"/>
    <text x="349" y="60" text-anchor="middle" class="sub">discover</text>
    <text x="349" y="132" text-anchor="middle" class="sub">consume</text>
    <text x="248" y="274" text-anchor="middle" class="sub">add.sh</text>
  </g>
</svg>
</div>

## Run

```bash
cd examples/connectors/flink-dynamic-kafka
docker compose up -d --build
./seed.sh
./verify.sh a b
```

| Service | Host |
| --- | --- |
| PicoMQ | `http://localhost:9090` |
| Kafka listener | `localhost:9092` |
| Files | `./out/fleets.<id>/` |

`--build` compiles the Flink job image.

## Job

`flink-job/src/main/java/picomq/example/IngestionJob.java`:

| Piece | Setting |
| --- | --- |
| Discovery | `HttpKafkaMetadataService` polls `GET /?prefix=/fleets.` on the Pico HTTP listener |
| Source | `DynamicKafkaSource`, bootstrap `pico:9092`, `allow.auto.create.topics=false` |
| Sink | `FileSink` under `./out`, bucket by topic name |
| Checkpoint | 2s, so files appear without a shutdown |

## Add a stream

```bash
./add.sh          # pico append /fleets.c
./verify.sh a b c
```

Topic `fleets.c` appears after the next poll.

## Remove a stream

```bash
./remove.sh       # pico delete /fleets.a, append to /fleets.b
./verify.sh b c
```

Topic `fleets.a` is dropped after the next poll. `fleets.b` keeps receiving.
