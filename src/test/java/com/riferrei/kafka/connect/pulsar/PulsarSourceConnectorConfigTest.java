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
        assertEquals(-1, config.getInt(BATCH_MAX_NUM_MESSAGES_CONFIG));
        assertEquals(20480, config.getInt(BATCH_MAX_NUM_BYTES_CONFIG));
        assertEquals(100, config.getInt(BATCH_TIMEOUT_CONFIG));
    }

}
