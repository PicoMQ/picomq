# Flink agents to S3 Tables

The [AI SDK app](/docs/examples/agents/ai-sdk) writes chat and agent runs to PicoMQ streams under `/examples/agents/ai-sdk/`. Managed Flink discovers those streams by prefix, consumes them over the Kafka listener of the [AWS harness](/docs/operations/deployment/aws), and writes two Iceberg tables in S3 Tables. The app never changes and never sees Iceberg.

Source: [`examples/connectors/flink-agents-s3-tables`](https://github.com/PicoMQ/picomq/tree/main/examples/connectors/flink-agents-s3-tables).

<div class="pico-diagram">
<svg viewBox="0 0 1240 342" width="1240" role="img" aria-label="The ai-sdk server on a laptop appends to PicoMQ through the internal ALB. Managed Flink polls the HTTP listener for streams under the prefix, consumes them over the Kafka NLB, routes chat and agent topics to two Iceberg sinks, and commits to S3 Tables once per checkpoint. DuckDB or Athena query the tables.">
  <defs>
    <marker id="fsa" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(20 6)">
    <rect x="-14" y="14" width="228" height="112" class="edge-soft"/>
<text x="-4" y="30" class="sub">laptop</text>
    <rect x="274" y="14" width="322" height="244" class="edge-soft"/>
<text x="284" y="30" class="sub">pico, harness vpc</text>
    <rect x="660" y="14" width="260" height="312" class="edge-soft"/>
<text x="670" y="30" class="sub">managed flink</text>
    <rect x="970" y="14" width="228" height="312" class="edge-soft"/>
<text x="980" y="30" class="sub">s3 tables</text>
    <rect x="0" y="40" width="200" height="56" class="box"/>
<text x="100" y="64" text-anchor="middle" class="label">ai-sdk :3456</text>
<text x="100" y="82" text-anchor="middle" class="sub">SSM tunnel to the alb</text>
    <rect x="288" y="40" width="294" height="56" class="box"/>
<text x="435" y="64" text-anchor="middle" class="label">http :4437, internal alb</text>
<text x="435" y="82" text-anchor="middle" class="sub">pico-{n}.picomq.internal</text>
    <rect x="288" y="112" width="294" height="56" class="box"/>
<text x="435" y="136" text-anchor="middle" class="label">kafka :9092, internal nlb</text>
<text x="435" y="154" text-anchor="middle" class="sub">kafka.picomq.internal</text>
    <rect x="288" y="208" width="142" height="36" class="box-accent"/>
<text x="359" y="231" text-anchor="middle" class="label">chat/{id}</text>
    <rect x="440" y="208" width="142" height="36" class="box-accent"/>
<text x="511" y="231" text-anchor="middle" class="label">agent/run-{id}</text>
    <rect x="674" y="40" width="232" height="56" class="box"/>
<text x="790" y="64" text-anchor="middle" class="label">HttpKafkaMetadataService</text>
<text x="790" y="82" text-anchor="middle" class="sub">GET /?prefix=, bearer token</text>
    <rect x="674" y="112" width="232" height="56" class="box"/>
<text x="790" y="136" text-anchor="middle" class="label">DynamicKafkaSource</text>
<text x="790" y="154" text-anchor="middle" class="sub">committed offsets</text>
    <rect x="674" y="184" width="232" height="56" class="box"/>
<text x="790" y="208" text-anchor="middle" class="label">Rows</text>
<text x="790" y="226" text-anchor="middle" class="sub">route by topic prefix</text>
    <rect x="674" y="256" width="232" height="56" class="box"/>
<text x="790" y="280" text-anchor="middle" class="label">IcebergSink x2</text>
<text x="790" y="298" text-anchor="middle" class="sub">commit per 60s checkpoint</text>
    <rect x="984" y="40" width="200" height="56" class="box"/>
<text x="1084" y="64" text-anchor="middle" class="label">duckdb, athena</text>
<text x="1084" y="82" text-anchor="middle" class="sub">REST catalog, sigv4</text>
    <rect x="984" y="184" width="200" height="36" class="box-accent"/>
<text x="1084" y="207" text-anchor="middle" class="label">agents.conversations</text>
    <rect x="984" y="266" width="200" height="36" class="box-accent"/>
<text x="1084" y="289" text-anchor="middle" class="label">agents.agent_events</text>
    <path d="M200 68 L284 68" class="edge" marker-end="url(#fsa)"/>
    <path d="M674 68 L586 68" class="edge" marker-end="url(#fsa)"/>
    <path d="M674 140 L586 140" class="edge" marker-end="url(#fsa)"/>
    <path d="M790 96 L790 108" class="edge" marker-end="url(#fsa)"/>
    <path d="M790 168 L790 180" class="edge" marker-end="url(#fsa)"/>
    <path d="M790 240 L790 252" class="edge" marker-end="url(#fsa)"/>
    <path d="M906 284 L944 284" class="edge"/>
<path d="M944 284 L944 202" class="edge"/>
<path d="M944 202 L980 202" class="edge" marker-end="url(#fsa)"/>
<path d="M944 284 L980 284" class="edge" marker-end="url(#fsa)"/>
    <path d="M1084 184 L1084 100" class="edge" marker-end="url(#fsa)"/>
    <text x="242" y="60" text-anchor="middle" class="sub">append</text>
    <text x="630" y="60" text-anchor="middle" class="sub">discover</text>
    <text x="630" y="132" text-anchor="middle" class="sub">consume</text>
    <text x="948" y="306" text-anchor="middle" class="sub">commit</text>
    <text x="1096" y="146" class="sub">query</text>
  </g>
</svg>
</div>

| Streams | Table | Columns |
| --- | --- | --- |
| `chat/{id}` | `agents.conversations` | `stream, seq, role, content, ts` |
| `agent/run-{id}` | `agents.agent_events` | `stream, seq, type, step_index, finish_reason, total_tokens, tools, ts` |

`stream` is the PicoMQ stream name, `seq` the record position, `ts` the record timestamp. `multi/*` streams are discovered and ignored.

## Run

The harness is applied and the AI SDK app is running against it from your laptop, see `examples/agents/ai-sdk/harness.md`.

```bash
cd examples/connectors/flink-agents-s3-tables
export AWS_PROFILE=picomq-support AWS_REGION=us-east-1

mvn -f flink-job -q -DskipTests package
cp terraform/backend.hcl.example terraform/backend.hcl
terraform -chdir=terraform init -backend-config=backend.hcl
terraform -chdir=terraform apply -auto-approve
```

Use the chat and agent pages, then:

```bash
./verify.sh
```

## Stack

The example ships its own Terraform. It reads the harness state from the same S3 bucket and never modifies it.

| Read from harness | Used for |
| --- | --- |
| `vpc_id`, `private_subnet_ids` | Flink security group and `vpc_configuration` |
| `endpoints["1"]` | `PICO_HTTP` for prefix discovery through the ALB |
| `kafka_bootstrap` | `KAFKA_BOOTSTRAP`, `kafka.picomq.internal:9092` |
| `bootstrap_secret_arn` | `PICO_TOKEN_SECRET_ARN`, fetched by the job at start |

| Created | Setting |
| --- | --- |
| S3 Tables bucket, namespace `agents`, two tables | schemas above, created before the job starts |
| Versioned code bucket and jar object | rebuilt jar redeploys the app on the next apply |
| Managed Flink application | `FLINK-2_3`, parallelism 1, snapshots on, private subnets |
| IAM role | jar read, `s3tables:*` on the bucket, `GetSecretValue` on the bootstrap secret, logs, ENI |

## Job

`flink-job/src/main/java/picomq/example/AgentsJob.java`:

| Piece | Setting |
| --- | --- |
| Discovery | `HttpKafkaMetadataService` polls `GET /?prefix=/examples/agents/ai-sdk/` with the bootstrap token, 10s |
| Source | `DynamicKafkaSource`, committed group offsets, earliest on a cold start |
| Routing | topic `examples.agents.ai-sdk.chat.*` to `conversations`, `agent.*` to `agent_events` |
| Sink | `IcebergSink` per table, REST catalog `https://s3tables.<region>.amazonaws.com/iceberg`, sigv4 |
| Checkpoint | 60s, one Iceberg commit per checkpoint |
| Config | MSF property group `picomq`, env fallback for local runs |

<div class="pico-diagram">
<svg viewBox="0 0 726 156" width="726" role="img" aria-label="One agent run stream with run_start, two steps and run_end becomes four rows in agent_events keyed by stream and seq.">
  <defs>
    <marker id="fsb" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <g transform="translate(20 -10)">
    <rect x="0" y="30" width="156" height="56" class="box-accent"/>
<text x="78" y="54" text-anchor="middle" class="label">run_start</text>
<text x="78" y="72" text-anchor="middle" class="sub">seq 0</text>
    <rect x="176" y="30" width="156" height="56" class="box"/>
<text x="254" y="54" text-anchor="middle" class="label">step</text>
<text x="254" y="72" text-anchor="middle" class="sub">seq 1, tools</text>
    <rect x="352" y="30" width="156" height="56" class="box"/>
<text x="430" y="54" text-anchor="middle" class="label">step</text>
<text x="430" y="72" text-anchor="middle" class="sub">seq 2, finish_reason</text>
    <rect x="528" y="30" width="156" height="56" class="box-accent"/>
<text x="606" y="54" text-anchor="middle" class="label">run_end</text>
<text x="606" y="72" text-anchor="middle" class="sub">seq 3, total_tokens</text>
    <path d="M156 58 L172 58" class="edge" marker-end="url(#fsb)"/>
    <path d="M332 58 L348 58" class="edge" marker-end="url(#fsb)"/>
    <path d="M508 58 L524 58" class="edge" marker-end="url(#fsb)"/>
    <path d="M78 86 L78 118" class="edge" marker-end="url(#fsb)"/>
    <path d="M254 86 L254 118" class="edge" marker-end="url(#fsb)"/>
    <path d="M430 86 L430 118" class="edge" marker-end="url(#fsb)"/>
    <path d="M606 86 L606 118" class="edge" marker-end="url(#fsb)"/>
    <rect x="0" y="122" width="684" height="36" class="box"/>
<text x="342" y="145" text-anchor="middle" class="label">agents.agent_events, four rows for one stream</text>
  </g>
</svg>
</div>

## Query

`sql/duckdb.sql` through `verify.sh`, attached with `ENDPOINT_TYPE s3_tables`:

```sql
SELECT stream AS run,
       count(*) FILTER (type = 'step') AS steps,
       string_agg(tools, ',') FILTER (tools IS NOT NULL) AS tools,
       max(total_tokens) AS total_tokens
FROM lake.agents.agent_events
GROUP BY stream;
```

Athena needs the S3 Tables catalog mounted once per account and region:

```bash
aws glue create-catalog --name s3tablescatalog --catalog-input '{
  "FederatedCatalog": {"Identifier": "arn:aws:s3tables:<region>:<account>:bucket/*", "ConnectionName": "aws:s3tables"},
  "CreateDatabaseDefaultPermissions": [{"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": ["ALL"]}],
  "CreateTableDefaultPermissions": [{"Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": ["ALL"]}],
  "AllowFullTableExternalDataAccess": "True"
}'
```

```sql
SELECT * FROM "s3tablescatalog/<table bucket>"."agents"."conversations" ORDER BY stream, seq
```

## Restarts

Snapshots are enabled on the application and the source starts from committed group offsets, so a redeploy or a new jar resumes where it left off. No rows are re-appended.

## Teardown

```bash
terraform -chdir=terraform destroy -auto-approve
```

Then the harness. PicoMQ also has an Iceberg sink connector, see [Fleet telematics to Iceberg](/docs/examples/connectors/fleet-telematics-iceberg). This example goes through the Kafka protocol into a third-party engine on purpose.
