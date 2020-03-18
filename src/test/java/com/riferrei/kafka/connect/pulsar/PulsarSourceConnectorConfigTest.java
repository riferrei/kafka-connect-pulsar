package com.riferrei.kafka.connect.pulsar;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceConnectorConfigTest {

    private static final String SERVICE_URL_VALUE = "pulsar://localhost:6650";
    private static final String SERVICE_HTTP_URL_VALUE = "http://localhost:8080";

    @Test
    public void basicOptionsAreMandatory() {
        assertThrows(ConfigException.class, () -> {
            Map<String, String> props = new HashMap<>();
            new PulsarSourceConnectorConfig(props);
        });
    }

    public void checkingBatchingDefaults() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, SERVICE_URL_VALUE);
        props.put(SERVICE_HTTP_URL_CONFIG, SERVICE_HTTP_URL_VALUE);
        PulsarSourceConnectorConfig config = new PulsarSourceConnectorConfig(props);
        assertEquals(10, config.getInt(BATCH_MAX_NUM_MESSAGES_CONFIG));
        assertEquals(1024, config.getInt(BATCH_MAX_NUM_BYTES_CONFIG));
        assertEquals(1000, config.getInt(BATCH_TIMEOUT_CONFIG));
    }

}
