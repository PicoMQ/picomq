package picomq.example;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.kafka.clients.CommonClientConfigs;

public final class HttpKafkaMetadataService implements KafkaMetadataService {
    public static final String CLUSTER_ID = "picomq";
    public static final String STREAM_ID = "ingestion";

    private static final Pattern NAME = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"");
    private static final int PAGE = 100;

    private final String picoBase;
    private final String prefix;
    private final String bootstrap;
    private transient HttpClient http;
    private volatile Set<String> lastTopics = Set.of();

    public HttpKafkaMetadataService(String picoBase, String prefix, String bootstrap) {
        this.picoBase = picoBase.endsWith("/") ? picoBase.substring(0, picoBase.length() - 1) : picoBase;
        this.prefix = prefix;
        this.bootstrap = bootstrap;
    }

    @Override
    public Set<KafkaStream> getAllStreams() {
        Set<String> topics = listTopics();
        lastTopics = topics;
        if (topics.isEmpty()) {
            return Set.of();
        }
        return Set.of(stream(topics));
    }

    @Override
    public Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
        KafkaStream stream = stream(listTopics());
        Map<String, KafkaStream> described = new HashMap<>();
        for (String streamId : streamIds) {
            if (STREAM_ID.equals(streamId) && !stream.getClusterMetadataMap().isEmpty()) {
                Set<String> topics = stream.getClusterMetadataMap().get(CLUSTER_ID).getTopics();
                if (!topics.isEmpty()) {
                    described.put(streamId, stream);
                }
            }
        }
        return described;
    }

    @Override
    public boolean isClusterActive(String kafkaClusterId) {
        return CLUSTER_ID.equals(kafkaClusterId);
    }

    @Override
    public void close() {}

    private KafkaStream stream(Set<String> topics) {
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        ClusterMetadata cluster = new ClusterMetadata(topics, properties);
        return new KafkaStream(STREAM_ID, Map.of(CLUSTER_ID, cluster));
    }

    private Set<String> listTopics() {
        try {
            Set<String> topics = new HashSet<>();
            String after = null;
            for (int page = 0; page < 64; page++) {
                String body = getPage(after);
                Matcher matcher = NAME.matcher(body);
                String last = null;
                while (matcher.find()) {
                    last = matcher.group(1);
                    String topic = topicFor(last);
                    if (topic != null) {
                        topics.add(topic);
                    }
                }
                if (!body.contains("\"has_more\":true") && !body.contains("\"has_more\": true")) {
                    lastTopics = topics;
                    return topics;
                }
                if (last == null) {
                    lastTopics = topics;
                    return topics;
                }
                after = last;
            }
            lastTopics = topics;
            return topics;
        } catch (IOException | InterruptedException error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return lastTopics;
        }
    }

    private String getPage(String after) throws IOException, InterruptedException {
        StringBuilder uri = new StringBuilder(picoBase)
                .append("/?prefix=")
                .append(URLEncoder.encode(prefix, StandardCharsets.UTF_8))
                .append("&limit=")
                .append(PAGE);
        if (after != null) {
            uri.append("&start_after=").append(URLEncoder.encode(after, StandardCharsets.UTF_8));
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(uri.toString()))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> response = http().send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("list " + response.statusCode());
        }
        return response.body();
    }

    private HttpClient http() {
        if (http == null) {
            http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        }
        return http;
    }

    static String topicFor(String streamName) {
        if (!streamName.startsWith("/")) {
            return null;
        }
        String candidate = streamName.substring(1).replace('/', '.');
        if (candidate.isEmpty() || candidate.equals(".") || candidate.equals("..") || candidate.length() > 249) {
            return null;
        }
        for (int i = 0; i < candidate.length(); i++) {
            char ch = candidate.charAt(i);
            if (!(Character.isLetterOrDigit(ch) || ch == '.' || ch == '_' || ch == '-')) {
                return null;
            }
        }
        return candidate;
    }
}
