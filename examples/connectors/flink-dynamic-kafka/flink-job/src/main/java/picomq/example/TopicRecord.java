package picomq.example;

public class TopicRecord {
    public String topic;
    public String payload;

    public TopicRecord() {}

    public TopicRecord(String topic, String payload) {
        this.topic = topic;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return payload;
    }
}
