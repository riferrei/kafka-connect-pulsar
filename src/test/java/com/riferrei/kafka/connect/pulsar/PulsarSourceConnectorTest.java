package com.riferrei.kafka.connect.pulsar;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceConnectorTest extends AbstractBasicTest {

    @Test
    public void connectorVersionShouldMatch() {
        String version = VersionUtil.getVersion();
        assertEquals(version, new PulsarSourceConnector().version());
    }

    @Test
    public void checkClassTask() {
        Class<? extends Task> taskClass = new PulsarSourceConnector().taskClass();
        assertEquals(PulsarSourceTask.class, taskClass);
    }

    @Test
    public void checkMissingTopicDefinition() {
        assertThrows(ConnectException.class, () -> {
            Map<String, String> props = new HashMap<>();
            props.put(SERVICE_URL_CONFIG, serviceUrlValue);
            props.put(SERVICE_HTTP_URL_CONFIG, serviceHttpUrlValue);
            new PulsarSourceConnector().validate(props);
        });
    }

    @Test
    public void patternShouldCreateOnlyOneTask() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, serviceUrlValue);
        props.put(SERVICE_HTTP_URL_CONFIG, serviceHttpUrlValue);
        props.put(TOPIC_PATTERN_CONFIG, topicPatternValue);
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertEquals(1, connector.taskConfigs(1).size());
    }

    @Test
    public void whiteListShouldCreateMultipleTasks() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, serviceUrlValue);
        props.put(SERVICE_HTTP_URL_CONFIG, serviceHttpUrlValue);
        props.put(TOPIC_WHITELIST_CONFIG, listToString(topic[0], topic[1], topic[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertEquals(3, connector.taskConfigs(3).size());
    }

    @Test
    public void blackListUsageShouldChangeWhiteList() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, serviceUrlValue);
        props.put(SERVICE_HTTP_URL_CONFIG, serviceHttpUrlValue);
        props.put(TOPIC_WHITELIST_CONFIG, listToString(topic[0], topic[1], topic[2]));
        props.put(TOPIC_BLACKLIST_CONFIG, listToString(topic[2], topic[3], topic[4]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertEquals(2, connector.taskConfigs(6).size());
    }

    @Test
    public void whiteListShouldTakePrecedenceOverPattern() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, serviceUrlValue);
        props.put(SERVICE_HTTP_URL_CONFIG, serviceHttpUrlValue);
        props.put(TOPIC_PATTERN_CONFIG, topicPatternValue);
        props.put(TOPIC_WHITELIST_CONFIG, listToString(topic[0], topic[1], topic[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertEquals(3, connector.taskConfigs(3).size());
    }

    @Test
    public void checkTaskCreationWithSmallerMaxTasks() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, serviceUrlValue);
        props.put(SERVICE_HTTP_URL_CONFIG, serviceHttpUrlValue);
        props.put(TOPIC_WHITELIST_CONFIG, listToString(topic[0], topic[1], topic[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
        assertEquals(listToString(topic[0], topic[1]), taskConfigs.get(0).get(TOPIC_NAMES));
        assertEquals(topic[2], taskConfigs.get(1).get(TOPIC_NAMES));
        
    }
    
}
