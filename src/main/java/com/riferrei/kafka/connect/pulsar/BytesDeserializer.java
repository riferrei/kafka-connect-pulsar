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

import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.Message;

public class BytesDeserializer implements Deserializer<byte[]> {

    @Override
    public SourceRecord deserialize(Message<byte[]> message,
        TopicNamingStrategy topicNamingStrategy) {
        String topic = TopicNameUtil.getTopic(
            message.getTopicName(), topicNamingStrategy);
        String offset = message.getMessageId().toString();
        return new SourceRecord(
            Collections.singletonMap(SOURCE_PARTITION, topic),
            Collections.singletonMap(SOURCE_OFFSET, offset),
            topic, Schema.BYTES_SCHEMA, message.getKeyBytes(),
            Schema.BYTES_SCHEMA, message.getData());
    }

}
