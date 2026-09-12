package picomq.example;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

public final class AgentsJob {
    public static void main(String[] args) throws Exception {
        Map<String, String> cfg = config();
        String pico = cfg.getOrDefault("PICO_HTTP", "http://pico:4437");
        String prefix = cfg.getOrDefault("PICO_PREFIX", "/examples/agents/ai-sdk/");
        String bootstrap = cfg.getOrDefault("KAFKA_BOOTSTRAP", "pico:9092");
        String region = cfg.getOrDefault("REGION", "us-east-1");
        String tableBucket = cfg.get("TABLE_BUCKET_ARN");
        String namespace = cfg.getOrDefault("NAMESPACE", "agents");
        String token = token(cfg, region);

        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.setProperty("group.id", "picomq-flink-agents");
        properties.setProperty("allow.auto.create.topics", "false");
        properties.setProperty("stream-metadata-discovery-interval-ms", cfg.getOrDefault("DISCOVERY_INTERVAL_MS", "10000"));
        properties.setProperty("stream-metadata-discovery-failure-threshold", "30");

        DynamicKafkaSource<TopicRecord> source = DynamicKafkaSource.<TopicRecord>builder()
                .setKafkaMetadataService(new HttpKafkaMetadataService(pico, prefix, bootstrap, token))
                .setStreamIds(Set.of(HttpKafkaMetadataService.STREAM_ID))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new TopicRecordDeserializer())
                .setProperties(properties)
                .build();

        String topicPrefix = prefix.substring(1).replace('/', '.');
        String chat = topicPrefix + "chat.";
        String agent = topicPrefix + "agent.";

        Map<String, String> catalog = new HashMap<>();
        catalog.put(CatalogProperties.URI, "https://s3tables." + region + ".amazonaws.com/iceberg");
        catalog.put(CatalogProperties.WAREHOUSE_LOCATION, tableBucket);
        catalog.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        catalog.put("rest.auth.type", "sigv4");
        catalog.put("rest.signing-name", "s3tables");
        catalog.put("rest.signing-region", region);
        CatalogLoader loader = CatalogLoader.rest("s3tables", new Configuration(false), catalog);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.setParallelism(1);

        DataStream<TopicRecord> records = env.fromSource(source, WatermarkStrategy.noWatermarks(), "dynamic-kafka");

        DataStream<RowData> conversations = records
                .filter(r -> r.topic.startsWith(chat))
                .flatMap(new Rows.Conversation(), InternalTypeInfo.of(Rows.CONVERSATION));
        IcebergSink.forRowData(conversations)
                .tableLoader(TableLoader.fromCatalog(loader, TableIdentifier.of(namespace, "conversations")))
                .uidSuffix("conversations")
                .append();

        DataStream<RowData> events = records
                .filter(r -> r.topic.startsWith(agent))
                .flatMap(new Rows.AgentEvent(), InternalTypeInfo.of(Rows.AGENT_EVENT));
        IcebergSink.forRowData(events)
                .tableLoader(TableLoader.fromCatalog(loader, TableIdentifier.of(namespace, "agent_events")))
                .uidSuffix("agent_events")
                .append();

        env.execute("agents-s3-tables");
    }

    private static Map<String, String> config() {
        Map<String, String> cfg = new HashMap<>(System.getenv());
        try {
            Properties group = KinesisAnalyticsRuntime.getApplicationProperties().get("picomq");
            if (group != null) {
                group.stringPropertyNames().forEach(k -> cfg.put(k, group.getProperty(k)));
            }
        } catch (Exception ignored) {
        }
        return cfg;
    }

    private static String token(Map<String, String> cfg, String region) {
        String arn = cfg.get("PICO_TOKEN_SECRET_ARN");
        if (arn == null || arn.isBlank()) {
            return cfg.get("PICO_TOKEN");
        }
        try (SecretsManagerClient client = SecretsManagerClient.builder().region(Region.of(region)).build()) {
            return client.getSecretValue(r -> r.secretId(arn)).secretString();
        }
    }
}
