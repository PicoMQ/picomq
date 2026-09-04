# Routing and templating

Kafka Connect assumes a topic is a big, pre-planned thing and a connector moves one or a few of them. PicoMQ assumes the opposite. Topics are cheap, a stream per user or per tenant is a normal design, and something has to decide which of many topics a record belongs to.

In the connectors that something is a routing rule on the way in and a destination template on the way out. Together they let a source scatter records into thousands of topics that a sink then gathers into a table each, with nobody enumerating the names in between.

<div class="pico-diagram">
<svg viewBox="0 30 720 230" width="720" role="img" aria-label="A source plugin emits records with a user_id field. The router applies the template users.{value} and produces each record into users.17, users.42 or users.91. A sink subscribed to the pattern users\..* consumes all of them.">
  <defs>
    <marker id="arrr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="110" width="130" height="56" class="box"/>
  <text x="85" y="134" text-anchor="middle" class="label">source</text>
  <text x="85" y="152" text-anchor="middle" class="sub">{ user_id: 42 }</text>
  <rect x="200" y="104" width="150" height="70" class="box-accent"/>
  <text x="275" y="126" text-anchor="middle" class="label">router</text>
  <text x="275" y="144" text-anchor="middle" class="sub">field user_id</text>
  <text x="275" y="162" text-anchor="middle" class="sub">users.{value}</text>
  <rect x="410" y="50" width="120" height="40" class="box"/>
  <text x="470" y="75" text-anchor="middle" class="label">users.17</text>
  <rect x="410" y="118" width="120" height="40" class="box"/>
  <text x="470" y="143" text-anchor="middle" class="label">users.42</text>
  <rect x="410" y="186" width="120" height="40" class="box"/>
  <text x="470" y="211" text-anchor="middle" class="label">users.91</text>
  <rect x="580" y="110" width="120" height="56" class="box"/>
  <text x="640" y="134" text-anchor="middle" class="label">sink</text>
  <text x="640" y="152" text-anchor="middle" class="sub">users\..*</text>
  <path d="M150 138 L192 138" class="edge" marker-end="url(#arrr)"/>
  <path d="M350 130 L402 74" class="edge" marker-end="url(#arrr)"/>
  <path d="M350 138 L402 138" class="edge" marker-end="url(#arrr)"/>
  <path d="M350 146 L402 202" class="edge" marker-end="url(#arrr)"/>
  <path d="M530 74 L572 130" class="edge" marker-end="url(#arrr)"/>
  <path d="M530 138 L572 138" class="edge" marker-end="url(#arrr)"/>
  <path d="M530 202 L572 146" class="edge" marker-end="url(#arrr)"/>
  <text x="470" y="245" text-anchor="middle" class="sub">created on first use when create_topics = true</text>
</svg>
</div>

## Routing rules on sources

The `topic` of a source's `[[topics]]` block is either a literal name or a rule. A rule names a strategy, says where to find the value, and gives a template with a `{value}` placeholder.

```toml
topic = "orders"
topic = { strategy = "field", path = "user.id", template = "users.{value}" }
topic = { strategy = "header", header = "tenant", template = "tenant.{value}", fallback = "tenant.unknown" }
topic = { strategy = "key", template = "keys.{value}" }
topic = { strategy = "hash", path = "user.id", buckets = 16, template = "shard.{value}" }
```

| Strategy | Value | Requires |
| --- | --- | --- |
| `static` | The literal `name`. Same as writing a bare string | `name` |
| `field` | A `path` into a JSON payload. Dotted for nested objects, numeric for array indices | `path`, JSON payload |
| `header` | The record header named `header`, read as UTF-8 | `header` |
| `key` | The record key, read as UTF-8 | A keyed record |
| `hash` | murmur2 of the field, header or key, masked to 31 bits, modulo `buckets`. The same key lands where a Kafka partitioner would put it | `buckets` plus one of the above |

How the value is derived from a JSON field.

| At the path | Becomes |
| --- | --- |
| `"acme"` | `acme` |
| `42` | `42` |
| `true` | `true` |
| `null`, an object, an array, or nothing | Missing |

A missing value is a routing failure.

- With a `fallback`, the record goes there.
- Without one, the whole batch is nacked and the source re-reads it. A record silently dropped is worse than a source that stops and says why.

`fallback` is the right choice for optional fields. Its absence is the right choice for a field that must be present.

### Sanitising

The substituted template becomes a legal topic name before PicoMQ sees it.

| Input | Rule |
| --- | --- |
| Letters, digits, `.`, `_`, `-` | Kept |
| Anything else | Replaced by `_` |
| Surrounding whitespace | Trimmed |
| Length | Cut at 249 characters |
| Empty after all that | Treated as missing |

A `tenant` of `acme corp` produces `tenant.acme_corp`, and a sink pattern has to expect the sanitised form.

### Creating topics

With `create_topics = true` the runtime creates each routed topic through the admin API the first time a record needs it, and remembers that it did. Creation is one round trip per new topic, paid once over the life of the runtime.

Without it, a record for a topic that does not exist fails the batch. That is the setting for deployments where topics are provisioned by something else.

## Destination templates on sinks

A sink's destination is the mirror of a source's topic. It is a table, collection, index, measurement, key prefix or URL, and it is either a literal or a template resolved from the topic each batch arrived on.

| Placeholder | Resolves to |
| --- | --- |
| `{topic}` | The whole topic name |
| `{topic_segment[n]}` | Segment `n` of the name split on `.`, counting from zero |
| `{topic_segment[-n]}` | Segment `n` counting from the end |

Only `.` separates segments. A hyphenated name such as `orders-eu` is one segment, so `{topic_segment[-1]}` returns the whole name. Route with dotted names when a sink template needs to pick a part out.

```toml
target_table = "events"                       # everything into one table
target_table = "events_{topic}"               # one table per topic
target_table = "{topic_segment[0]}_events"    # first dot-separated segment
target_table = "{topic_segment[-1]}"          # last segment
```

A topic with fewer segments than the template asks for fails the batch rather than producing a partial name.

<div class="pico-diagram">
<svg viewBox="0 30 720 200" width="720" role="img" aria-label="The topic orders.eu.2026 is split on dots into three segments. The template {topic_segment[0]}_{topic_segment[-1]} takes the first and last segment and resolves to orders_2026.">
  <defs>
    <marker id="arrseg" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="80" width="170" height="50" class="box-accent"/>
  <text x="105" y="101" text-anchor="middle" class="label">topic</text>
  <text x="105" y="119" text-anchor="middle" class="sub">orders.eu.2026</text>
  <rect x="250" y="50" width="90" height="36" class="box"/>
  <text x="295" y="73" text-anchor="middle" class="label">orders</text>
  <text x="295" y="44" text-anchor="middle" class="sub">[0] or [-3]</text>
  <rect x="250" y="102" width="90" height="36" class="box"/>
  <text x="295" y="125" text-anchor="middle" class="label">eu</text>
  <rect x="250" y="154" width="90" height="36" class="box"/>
  <text x="295" y="177" text-anchor="middle" class="label">2026</text>
  <text x="295" y="210" text-anchor="middle" class="sub">[2] or [-1]</text>
  <path d="M190 100 L242 68" class="edge" marker-end="url(#arrseg)"/>
  <path d="M190 105 L242 120" class="edge" marker-end="url(#arrseg)"/>
  <path d="M190 110 L242 172" class="edge" marker-end="url(#arrseg)"/>
  <rect x="400" y="80" width="310" height="50" class="box"/>
  <text x="555" y="101" text-anchor="middle" class="label">{topic_segment[0]}_{topic_segment[-1]}</text>
  <text x="555" y="119" text-anchor="middle" class="sub">orders_2026</text>
  <path d="M340 68 L392 100" class="edge" marker-end="url(#arrseg)"/>
  <path d="M340 172 L392 110" class="edge" marker-end="url(#arrseg)"/>
</svg>
</div>

### What each sink does with the name

Topic names can carry `-` and `.` that many identifiers cannot. Each sink turns the resolved template into something its destination accepts, and the two approaches differ in a way worth knowing.

| Approach | Example sink | Topic `orders.eu` becomes |
| --- | --- | --- |
| Quote verbatim | Postgres | A table literally called `"orders.eu"`, which every later query has to quote |
| Rewrite | Doris | `orders_eu`. Anything outside `[A-Za-z0-9_]` becomes `_`, and a leading digit gets a `_` prefix |

The catalog page for each sink states which it does.

### Who creates the destination

| Sink | On a new topic |
| --- | --- |
| Postgres, ClickHouse, MongoDB, Meilisearch, SurrealDB, Redshift, Elasticsearch, Quickwit | Checks for the destination, creates it if the sink's create option allows, caches the fact so later batches pay nothing |
| Doris | Loads into an existing table. Every name the template can produce has to be provisioned first |
| Iceberg, Delta | Writes into a table whose schema is already in the catalog. Same requirement |
| InfluxDB, S3 | No notion of creating a measurement or prefix. The first write brings it into being |

## Patterns tie the two together

A source that routes by `user_id` produces into topics that did not exist when the sink was configured. The sink follows them with a `pattern`, which the runtime re-evaluates against the broker every two seconds. A newly created topic is being consumed within that window, from its earliest offset by default, so no records are missed between creation and subscription.

The whole design is three lines of configuration.

```toml
# source
topic = { strategy = "field", path = "user_id", template = "users.{value}" }
create_topics = true

# sink
pattern = 'users\..*'

# sink plugin_config
target_table = "user_{topic_segment[-1]}"
```

Users appear, topics appear, tables appear, and nobody wrote a list.

## Choosing a strategy

| Reach for | When |
| --- | --- |
| `field` | The routing key is in the record. The common case |
| `header` | The payload is opaque or the routing was decided upstream, a tenant id stamped by an API gateway for instance |
| `key` | The source already sets a Kafka key with meaning |
| `hash` | The natural key has too many values to want a topic each. A million users into sixteen shards |

Bucket counts are fixed once data is flowing. Changing `buckets` reassigns most keys to different topics, which is fine for new data and confusing for anything that expected a key's history in one place. Pick a count with headroom.
