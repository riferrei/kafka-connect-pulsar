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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class TopicRegexMonitor extends Thread {

    private final Logger log = LoggerFactory.getLogger(TopicRegexMonitor.class);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private PulsarAdmin pulsarAdmin;
    private ConnectorContext context;
    private Pattern topicsPattern;
    private RegexSubscriptionMode regexSubscriptionMode;
    private long pollInterval;
    private List<String> topics;

    public TopicRegexMonitor(ConnectorContext context,
        String topicRegex, String regexSubscriptionMode,
        long pollInterval, String serviceHttpUrl) {
        this.context = context;
        this.topicsPattern = Pattern.compile(topicRegex);
        this.regexSubscriptionMode = RegexSubscriptionMode.valueOf(regexSubscriptionMode);
        this.pollInterval = pollInterval;
        try {
            pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(serviceHttpUrl)
                .build();
        } catch (PulsarClientException pce) {
            if (log.isErrorEnabled()) {
                log.error("Error while connecting to Pulsar admin service: ", pce);
            }
        }
        this.topics = getTopics();
    }

    @Override
    public void run() {
        log.info("Starting thread to monitor topic regex.");
        while (shutdownLatch.getCount() > 0) {
            try {
                List<String> currentRead = getTopics();
                if (!currentRead.equals(topics)) {
                    log.info("Previous list of topics: " + topics);
                    log.info("Current list of topics: " + currentRead);
                    log.info("Changes detected in the topics. Requesting reconfiguration...");
                    context.requestTaskReconfiguration();
                    topics = currentRead;
                }
                boolean shuttingDown = shutdownLatch.await(pollInterval, TimeUnit.MILLISECONDS);
                if (shuttingDown) {
                    return;
                }
            } catch (InterruptedException ie) {
                log.error("Unexpected InterruptedException, ignoring: ", ie);
            }
        }
    }

    public synchronized List<String> getTopics() {
        final List<String> topics = new ArrayList<>();
        try {
            Topics topicAdmin = pulsarAdmin.topics();
            TopicName topicName = TopicName.get(topicsPattern.pattern());
            NamespaceName namespaceName = topicName.getNamespaceObject();
            List<String> topicList = topicAdmin.getList(namespaceName.toString());
            if (topicList != null && !topicList.isEmpty()) {
                topicList = PulsarClientImpl.topicsPatternFilter(topicList, topicsPattern);
                switch (regexSubscriptionMode) {
                    case PersistentOnly:
                        topics.addAll(
                            topicList
                                .stream()
                                .filter(topic -> TopicName.get(topic).isPersistent())
                                .collect(Collectors.toList()));
                        break;
                    case NonPersistentOnly:
                        topics.addAll(
                            topicList
                                .stream()
                                .filter(topic -> !TopicName.get(topic).isPersistent())
                                .collect(Collectors.toList()));
                        break;
                    case AllTopics:
                        topics.addAll(topicList);
                        break;
                    default:
                        log.warn("Invalid value set for the property '%s': %s",
                            REGEX_SUBSCRIPTION_MODE_CONFIG, regexSubscriptionMode);
                        break;
                }
            }
        } catch (PulsarAdminException pae) {
            if (log.isErrorEnabled()) {
                log.error("Error while retrieving the topics: ", pae);
            }
        }
        return topics;
    }

    public void shutdown() {
        log.info("Shutting down the topic regex monitoring thread.");
        shutdownLatch.countDown();
        pulsarAdmin.close();
    }

}
