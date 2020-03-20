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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

public class TopicRegexMonitor extends Thread {

    private final Logger log = LoggerFactory.getLogger(TopicRegexMonitor.class);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private PulsarAdmin pulsarAdmin;
    private ConnectorContext context;
    private String topicRegex;
    private RegexSubscriptionMode filter;
    private long pollInterval;
    private List<String> topics;

    public TopicRegexMonitor(ConnectorContext context,
        String topicRegex, String regexSubscriptionMode,
        long pollInterval, String serviceHttpUrl) {
        this.context = context;
        this.topicRegex = topicRegex;
        this.filter = RegexSubscriptionMode.valueOf(regexSubscriptionMode);
        this.pollInterval = pollInterval;
        try {
            pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(serviceHttpUrl)
                .build();
        } catch (PulsarClientException pce) {
            if (log.isErrorEnabled()) {
                log.error("Exception thrown while connecting to Pulsar's admin service: ", pce);
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
        List<String> topics = null;
        try {
            Topics topicAdmin = pulsarAdmin.topics();
            TopicName topicName = TopicName.get(topicRegex);
            NamespaceName namespaceName = topicName.getNamespaceObject();
            List<String> tempList = topicAdmin.getList(namespaceName.toString());
            if (tempList != null && !tempList.isEmpty()) {
                topics = new ArrayList<>(tempList.size());
                for (String topic : tempList) {
                    topicName = TopicName.get(topic);
                    topic = topicName.getPartitionedTopicName();
                    if (filter.equals(RegexSubscriptionMode.PersistentOnly)) {
                        if (!topicName.isPersistent()) {
                            continue;
                        }
                    }
                    if (filter.equals(RegexSubscriptionMode.NonPersistentOnly)) {
                        if (topicName.isPersistent()) {
                            continue;
                        }
                    }
                    if (!topics.contains(topic)) {
                        topics.add(topic);
                    }
                }
            } else {
                topics = Collections.emptyList();
            }
        } catch (PulsarAdminException pae) {
            if (log.isErrorEnabled()) {
                log.error("Exception thrown while retrieving topics from namespace: ", pae);
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
