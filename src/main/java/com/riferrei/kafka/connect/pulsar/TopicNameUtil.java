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

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pulsar.common.naming.TopicName;

public final class TopicNameUtil {

    private static Logger log = LoggerFactory.getLogger(TopicNameUtil.class);

    public static String getTopic(String topicName,
        TopicNamingStrategy topicNamingStrategyOption) {
        TopicName tName = TopicName.get(topicName);
        if (tName.isPartitioned()) {
            topicName = tName.getPartitionedTopicName();
        }
        URI topic = null;
        try {
            topic = new URI(topicName);
        } catch (Exception ex) {
            if (log.isErrorEnabled()) {
                log.error("Error while parsing the topic %s", topicName);
            }
            return null;
        }
        if (topicNamingStrategyOption.equals(TopicNamingStrategy.NameOnly)) {
            String[] topicNameParts = topic.getPath().split("/");
            topicName = topicNameParts[topicNameParts.length - 1];
        } else if (topicNamingStrategyOption.equals(TopicNamingStrategy.FullyQualified)) {
            StringBuilder fullyQualifiedTopic = new StringBuilder();
            if (topic.getHost() != null && topic.getHost().length() > 0) {
                fullyQualifiedTopic.append(topic.getHost());
            }
            fullyQualifiedTopic.append(topic.getPath().replaceAll("/", "-"));
            topicName = fullyQualifiedTopic.toString();
        }
        return topicName;
    }

    private TopicNameUtil() {
    }

}
