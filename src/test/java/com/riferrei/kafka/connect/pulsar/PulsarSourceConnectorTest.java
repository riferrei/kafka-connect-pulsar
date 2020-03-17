package com.riferrei.kafka.connect.pulsar;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceConnectorTest {

    private static final String SERVICE_URL_VALUE = "pulsar://localhost:6650";
    private static final String SERVICE_HTTP_URL_VALUE = "http://localhost:8080";
    private static final String TOPIC_PATTERN_VALUE = "persistent://public/default/.*";

    private static final String[] TOPIC = {
        "topic-1", "topic-2", "topic-3",
        "topic-4", "topic-5", "topic-6"
    };

    private String toString(String... topics) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < topics.length; i++) {
            if (i == topics.length - 1) {
                sb.append(topics[i]);
            } else {
                sb.append(topics[i]).append(", ");
            }
        }
        return sb.toString();
    }

    @Test
    public void testVersionShouldMatch() {
        String version = VersionUtil.getVersion();
        assertSame(version, new PulsarSourceConnector().version());
    }

    @Test
    public void testMissingTopicDefinition() {
        Assertions.assertThrows(ConnectException.class, () -> {
            Map<String, String> props = new HashMap<>();
            props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
            props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
            new PulsarSourceConnector().validate(props);
        });
    }

    @Test
    public void testTasksConfigWithTopicPattern() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_PATTERN_CONFIG, TOPIC_PATTERN_VALUE);
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertSame(1, connector.taskConfigs(1).size());
    }

    @Test
    public void testTasksConfigWithTopicWhitelist() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_WHITELIST_CONFIG, toString(TOPIC[0], TOPIC[1], TOPIC[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertSame(3, connector.taskConfigs(3).size());
    }

    @Test
    public void testTasksConfigWithTopicBlacklist() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_WHITELIST_CONFIG, toString(TOPIC[0], TOPIC[1], TOPIC[2]));
        props.put(TOPIC_BLACKLIST_CONFIG, toString(TOPIC[2], TOPIC[3], TOPIC[4]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertSame(2, connector.taskConfigs(6).size());
    }

    @Test
    public void testWhitelistPriorityOverTopicPattern() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_PATTERN_CONFIG, TOPIC_PATTERN_VALUE);
        props.put(TOPIC_WHITELIST_CONFIG, toString(TOPIC[0], TOPIC[1], TOPIC[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertSame(3, connector.taskConfigs(3).size());
    }

    @Test
    public void testTasksConfigWithSmallerMaxTasks() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_WHITELIST_CONFIG, toString(TOPIC[0], TOPIC[1], TOPIC[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
        Assertions.assertEquals(toString(TOPIC[0], TOPIC[1]), taskConfigs.get(0).get(TOPIC_NAMES));
        Assertions.assertEquals(TOPIC[2], taskConfigs.get(1).get(TOPIC_NAMES));
        
    }
    
}
