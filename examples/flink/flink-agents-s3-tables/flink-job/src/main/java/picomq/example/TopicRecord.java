package picomq.example;

public class TopicRecord {
    public String topic;
    public long offset;
    public long timestampMs;
    public String payload;

    public TopicRecord() {}

    public TopicRecord(String topic, long offset, long timestampMs, String payload) {
        this.topic = topic;
        this.offset = offset;
        this.timestampMs = timestampMs;
        this.payload = payload;
    }
}
