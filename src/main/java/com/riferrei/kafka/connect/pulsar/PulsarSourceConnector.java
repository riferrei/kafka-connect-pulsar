package com.riferrei.kafka.connect.pulsar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import com.riferrei.kafka.connect.pulsar.util.Version;

import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceConnector extends SourceConnector {

    private Map<String, String> originalProps;
    private PulsarSourceConnectorConfig config;

    @Override
    public String version() {
        return Version.getVersion();
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
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> whiteList = config.getList(TOPIC_WHITELIST_CONFIG);
        List<String> blackList = config.getList(TOPIC_BLACKLIST_CONFIG);
        if (blackList != null && !blackList.isEmpty()) {
            whiteList.removeAll(blackList);
        }
        int numGroups = Math.min(whiteList.size(), maxTasks);
        List<List<String>> topicSources = ConnectorUtils.groupPartitions(whiteList, numGroups);
        List<Map<String, String>> taskConfigs = new ArrayList<>(topicSources.size());
        for (List<String> topicNames : topicSources) {
            Map<String, String> taskConfig = new HashMap<>(originalProps);
            taskConfig.put(TOPIC_NAMES_CONFIG, String.join(",", topicNames));
            taskConfigs.add(taskConfig);
        }
        return taskConfigs;
    }

}
