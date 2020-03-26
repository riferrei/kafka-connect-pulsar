/**

    Copyright © 2020 Ricardo Ferreira (riferrei@riferrei.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

*/

package com.riferrei.kafka.connect.pulsar;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceConnectorTest extends AbstractBasicTest {

    @Test
    public void connectorVersionShouldMatch() {
        String version = PropertiesUtil.getConnectorVersion();
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
            props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
            props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
            new PulsarSourceConnector().validate(props);
        });
    }

    @Test
    public void checkTaskCreationUsingWhitelist() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_WHITELIST_CONFIG, listToString(REUSABLE_TOPICS[0],
            REUSABLE_TOPICS[1], REUSABLE_TOPICS[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertEquals(3, connector.taskConfigs(3).size());
    }

    @Test
    public void checkBlacklistAppliedToWhitelist() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_WHITELIST_CONFIG, listToString(REUSABLE_TOPICS[0],
            REUSABLE_TOPICS[1], REUSABLE_TOPICS[2]));
        props.put(TOPIC_BLACKLIST_CONFIG, listToString(REUSABLE_TOPICS[2],
            REUSABLE_TOPICS[3], REUSABLE_TOPICS[4]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertEquals(2, connector.taskConfigs(3).size());
    }

    @Test
    public void checkTaskCreationUsingRegex() {
        assertDoesNotThrow(() -> {
            produceBytesBasedMessages(REUSABLE_TOPICS[0], 1);
            produceBytesBasedMessages(REUSABLE_TOPICS[1], 1);
            produceBytesBasedMessages(REUSABLE_TOPICS[2], 1);
        });
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_REGEX_CONFIG, TOPIC_REGEX_VALUE);
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertEquals(3, connector.taskConfigs(3).size());
    }

    @Test
    public void checkBlacklistAppliedToRegex() {
        assertDoesNotThrow(() -> {
            produceBytesBasedMessages(REUSABLE_TOPICS[0], 1);
            produceBytesBasedMessages(REUSABLE_TOPICS[1], 1);
            produceBytesBasedMessages(REUSABLE_TOPICS[2], 1);
        });
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_REGEX_CONFIG, TOPIC_REGEX_VALUE);
        props.put(TOPIC_BLACKLIST_CONFIG, listToString(REUSABLE_TOPICS[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        assertEquals(2, connector.taskConfigs(3).size());
    }

    @Test
    public void checkTaskCreationWithSmallerMaxTasks() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        props.put(TOPIC_WHITELIST_CONFIG, listToString(REUSABLE_TOPICS[0],
            REUSABLE_TOPICS[1], REUSABLE_TOPICS[2]));
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
        assertEquals(listToString(REUSABLE_TOPICS[0], REUSABLE_TOPICS[1]),
            taskConfigs.get(0).get(TOPIC_NAMES));
        assertEquals(REUSABLE_TOPICS[2], taskConfigs.get(1).get(TOPIC_NAMES));
        
    }
    
}
