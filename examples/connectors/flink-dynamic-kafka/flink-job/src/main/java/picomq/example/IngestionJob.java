package picomq.example;

import java.util.Properties;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.kafka.clients.CommonClientConfigs;

public final class IngestionJob {
    public static void main(String[] args) throws Exception {
        String pico = env("PICO_HTTP", "http://pico:4437");
        String prefix = env("PICO_PREFIX", "/fleets.");
        String bootstrap = env("KAFKA_BOOTSTRAP", "pico:9092");
        String output = env("OUTPUT_PATH", "/out");
        String interval = env("DISCOVERY_INTERVAL_MS", "2000");

        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.setProperty("group.id", "picomq-flink-ingestion");
        properties.setProperty("allow.auto.create.topics", "false");
        properties.setProperty("stream-metadata-discovery-interval-ms", interval);
        properties.setProperty("stream-metadata-discovery-failure-threshold", "30");

        DynamicKafkaSource<TopicRecord> source = DynamicKafkaSource.<TopicRecord>builder()
                .setKafkaMetadataService(new HttpKafkaMetadataService(pico, prefix, bootstrap))
                .setStreamIds(Set.of(HttpKafkaMetadataService.STREAM_ID))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new TopicRecordDeserializer())
                .setProperties(properties)
                .build();

        FileSink<TopicRecord> sink = FileSink.forRowFormat(new Path(output), new SimpleStringEncoder<TopicRecord>("UTF-8"))
                .withBucketAssigner(new TopicBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointTimeout(30000);
        env.setParallelism(1);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "dynamic-kafka")
                .sinkTo(sink);
        env.execute("ingestion");
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
