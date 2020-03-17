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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceConnector extends SourceConnector {

    private Map<String, String> originalProps;
    private PulsarSourceConnectorConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void start(Map<String, String> originalProps) {
        this.originalProps = originalProps;
        this.config = new PulsarSourceConnectorConfig(originalProps);
    }

    @Override
    public void stop() {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PulsarSourceTask.class;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        List<ConfigValue> configValues = config.configValues();
        boolean missingTopicDefinition = true;
        for (ConfigValue configValue : configValues) {
            if (configValue.name().equals(TOPIC_WHITELIST_CONFIG)
                || configValue.name().equals(TOPIC_PATTERN_CONFIG)) {
                if (configValue.value() != null) {
                    missingTopicDefinition = false;
                    break;
                }
            }
        }
        if (missingTopicDefinition) {
            throw new ConnectException(String.format(
                "There is no topic definition in the "
                + "configuration. Either the property "
                + "'%s' or '%s' must be set in the configuration.",
                TOPIC_WHITELIST_CONFIG, TOPIC_PATTERN_CONFIG));
        }
        return config;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> taskConfigs = new ArrayList<>();
        String topicPattern = config.getString(TOPIC_PATTERN_CONFIG);
        // If the 'topic.pattern' configuration has been set, then
        // there will be only one task to handle the subscription.
        // Since there is no way to figure out how many topics the
        // consumer will subscribe to; we can't infer the number of
        // tasks to be created.
        if (topicPattern != null && topicPattern.length() > 0) {
            Map<String, String> taskConfig = new HashMap<>(originalProps);
            taskConfig.put(TOPIC_PATTERN, topicPattern);
            taskConfigs.add(taskConfig);
            return taskConfigs;
        } else {
            // Otherwise, just read the configuration set for topics to be
            // consumed to figure out how many tasks should be created. For
            // maximum parallelism, the value of 'tasks.max' should be set
            // to the same number of topics.
            List<String> whiteList = config.getList(TOPIC_WHITELIST_CONFIG);
            List<String> blackList = config.getList(TOPIC_BLACKLIST_CONFIG);
            if (blackList != null && !blackList.isEmpty()) {
                whiteList = new ArrayList<>(whiteList);
                whiteList.removeAll(blackList);
            }
            int numGroups = Math.min(whiteList.size(), maxTasks);
            List<List<String>> topicSources = ConnectorUtils.groupPartitions(whiteList, numGroups);
            for (List<String> topicSource : topicSources) {
                Map<String, String> taskConfig = new HashMap<>(originalProps);
                taskConfig.put(TOPIC_NAMES, String.join(",", topicSource));
                taskConfigs.add(taskConfig);
            }
        }
        return taskConfigs;
    }

}
