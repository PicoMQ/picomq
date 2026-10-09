package picomq.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;

public final class Rows {
    public static final RowType CONVERSATION = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new VarCharType(VarCharType.MAX_LENGTH),
                new LocalZonedTimestampType(6)
            },
            new String[] {"stream", "seq", "role", "content", "ts"});

    public static final RowType AGENT_EVENT = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new IntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new BigIntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new LocalZonedTimestampType(6)
            },
            new String[] {"stream", "seq", "type", "step_index", "finish_reason", "total_tokens", "tools", "ts"});

    private Rows() {}

    static String stream(String topic) {
        return "/" + topic.replace('.', '/');
    }

    static StringData str(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isTextual()) {
            return StringData.fromString(node.asText());
        }
        if (node.isArray()) {
            StringBuilder sb = new StringBuilder();
            for (JsonNode part : node) {
                JsonNode text = part.get("text");
                if (text != null && text.isTextual()) {
                    sb.append(text.asText());
                }
            }
            return StringData.fromString(sb.toString());
        }
        return StringData.fromString(node.toString());
    }

    public static final class Conversation extends RichFlatMapFunction<TopicRecord, RowData> {
        private transient ObjectMapper mapper;

        @Override
        public void flatMap(TopicRecord record, Collector<RowData> out) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            JsonNode node = mapper.readTree(record.payload);
            JsonNode role = node.get("role");
            if (role == null || !role.isTextual()) {
                return;
            }
            GenericRowData row = new GenericRowData(5);
            row.setField(0, StringData.fromString(stream(record.topic)));
            row.setField(1, record.offset);
            row.setField(2, StringData.fromString(role.asText()));
            row.setField(3, str(node.get("content")));
            row.setField(4, TimestampData.fromEpochMillis(record.timestampMs));
            out.collect(row);
        }
    }

    public static final class AgentEvent extends RichFlatMapFunction<TopicRecord, RowData> {
        private transient ObjectMapper mapper;

        @Override
        public void flatMap(TopicRecord record, Collector<RowData> out) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            JsonNode node = mapper.readTree(record.payload);
            JsonNode type = node.get("type");
            if (type == null || !type.isTextual()) {
                return;
            }
            GenericRowData row = new GenericRowData(8);
            row.setField(0, StringData.fromString(stream(record.topic)));
            row.setField(1, record.offset);
            row.setField(2, StringData.fromString(type.asText()));
            row.setField(3, node.hasNonNull("index") ? node.get("index").asInt() : null);
            row.setField(4, str(node.get("finishReason")));
            row.setField(5, node.hasNonNull("totalTokens") ? node.get("totalTokens").asLong() : null);
            row.setField(6, tools(node.get("toolCalls")));
            row.setField(7, TimestampData.fromEpochMillis(record.timestampMs));
            out.collect(row);
        }

        private static StringData tools(JsonNode calls) {
            if (calls == null || !calls.isArray() || calls.isEmpty()) {
                return null;
            }
            List<String> names = new ArrayList<>();
            for (JsonNode call : calls) {
                JsonNode tool = call.get("tool");
                if (tool != null && tool.isTextual()) {
                    names.add(tool.asText());
                }
            }
            return names.isEmpty() ? null : StringData.fromString(String.join(",", names));
        }
    }
}
