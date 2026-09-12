package picomq.example;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class TopicRecordDeserializer implements KafkaRecordDeserializationSchema<TopicRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TopicRecord> out) {
        byte[] value = record.value();
        if (value == null) {
            return;
        }
        long ts = record.timestamp() > 0 ? record.timestamp() : System.currentTimeMillis();
        out.collect(new TopicRecord(record.topic(), record.offset(), ts, new String(value, StandardCharsets.UTF_8)));
    }

    @Override
    public TypeInformation<TopicRecord> getProducedType() {
        return TypeInformation.of(TopicRecord.class);
    }
}
