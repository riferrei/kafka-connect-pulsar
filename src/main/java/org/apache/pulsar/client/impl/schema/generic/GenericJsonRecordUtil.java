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

package org.apache.pulsar.client.impl.schema.generic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.node.ArrayNode;

public final class GenericJsonRecordUtil {

    public static List<Object> getArray(GenericRecord record, Schema valueSchema) {
        List<Object> array = null;
        if (record instanceof GenericJsonRecord) {
            GenericJsonRecord jsonRecord = (GenericJsonRecord) record;
            JsonNode rootNode = jsonRecord.getJsonNode();
            if (rootNode instanceof ArrayNode) {
                ArrayNode arrayNode = (ArrayNode) rootNode;
                array = new ArrayList<Object>(arrayNode.size());
                for (int i = 0; i < arrayNode.size(); i++) {
                    JsonNode node = arrayNode.get(i);
                    switch (valueSchema.type()) {
                        case INT8: case INT16: case INT32:
                            array.add(node.asInt());
                            break;
                        case INT64:
                            array.add(node.asLong());
                            break;
                        case FLOAT32: case FLOAT64:
                            array.add(node.asDouble());
                            break;
                        case BOOLEAN:
                            array.add(node.asBoolean());
                            break;
                        case STRING: default:
                            array.add(node.asText());
                            break;
                    }
                }
            }
        }
        return array;
    }

    public static Map<Object, Object> getMap(GenericRecord record) {
        Map<Object, Object> map = null;
        if (record instanceof GenericJsonRecord) {
            GenericJsonRecord jsonRecord = (GenericJsonRecord) record;
            List<Field> keys = jsonRecord.getFields();
            map = new HashMap<>(keys.size());
            for (Field key : keys) {
                map.put(key.getName(), jsonRecord.getField(key));
            }
        }
        return map;
    }

    private GenericJsonRecordUtil() {
    }

}
