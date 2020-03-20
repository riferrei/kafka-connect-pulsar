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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.pulsar.common.naming.TopicName;

import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceConnector extends SourceConnector {

    private final Logger log = LoggerFactory.getLogger(PulsarSourceConnector.class);

    private Map<String, String> originalProps;
    private PulsarSourceConnectorConfig config;
    private TopicRegexMonitor topicRegexMonitor;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
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
            || configValue.name().equals(TOPIC_REGEX_CONFIG)) {
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
                TOPIC_WHITELIST_CONFIG, TOPIC_REGEX_CONFIG));
        }
        return config;
    }

    @Override
    public void start(Map<String, String> originalProps) {
        this.originalProps = originalProps;
        config = new PulsarSourceConnectorConfig(originalProps);
        if (config.getString(TOPIC_REGEX_CONFIG) != null) {
            String topicRegex = config.getString(TOPIC_REGEX_CONFIG);
            String regexSubscriptionMode = config.getString(REGEX_SUBSCRIPTION_MODE_CONFIG);
            long pollInterval = config.getLong(TOPIC_POLL_INTERVAL_MS_CONFIG);
            String serviceHttpUrl = config.getString(SERVICE_HTTP_URL_CONFIG);
            topicRegexMonitor = new TopicRegexMonitor(context, topicRegex,
                regexSubscriptionMode, pollInterval, serviceHttpUrl);
            topicRegexMonitor.start();
        }
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        List<String> topicList = config.getList(TOPIC_WHITELIST_CONFIG);
        if (topicList != null) {
            List<String> tempList = new ArrayList<>(topicList);
            topicList = new ArrayList<>(tempList.size());
            for (String topic : tempList) {
                TopicName topicName = TopicName.get(topic);
                topicList.add(topicName.getPartitionedTopicName());
            }
        } else {
            topicList = new ArrayList<>();
        }
        if (topicRegexMonitor != null) {
            List<String> regexList = topicRegexMonitor.getTopics();
            for (String topic : regexList) {
                if (!topicList.contains(topic)) {
                    topicList.add(topic);
                }
            }
        }
        List<String> blackList = config.getList(TOPIC_BLACKLIST_CONFIG);
        if (blackList != null) {
            List<String> tempList = new ArrayList<>(blackList);
            blackList = new ArrayList<>(tempList.size());
            for (String topic : tempList) {
                TopicName topicName = TopicName.get(topic);
                blackList.add(topicName.getPartitionedTopicName());
            }
            topicList.removeAll(blackList);
        }
        if (topicList.isEmpty()) {
            taskConfigs = Collections.emptyList();
            log.warn("No tasks will be created because there is zero topics to subscribe.");
        } else {
            int numGroups = Math.min(topicList.size(), maxTasks);
            List<List<String>> topicSources = ConnectorUtils.groupPartitions(topicList, numGroups);
            for (List<String> topicSource : topicSources) {
                Map<String, String> taskConfig = new HashMap<>(originalProps);
                taskConfig.put(TOPIC_NAMES, String.join(", ", topicSource));
                taskConfigs.add(taskConfig);
            }
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        topicRegexMonitor.shutdown();
    }

}
