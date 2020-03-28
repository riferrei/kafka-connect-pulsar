package com.riferrei.kafka.connect.pulsar;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;

public final class SchemaRegistry {

    private static Map<String, Schema> schemas = new HashMap<>();

    public static Schema createSchema(String topic,
        SchemaInfo schemaInfo, long schemaVersion) {
        Schema schema = buildSchema(schemaInfo.getSchemaDefinition());
        schemas.put(schemaKey(topic, schemaVersion), schema);
        return schema;
    }

    public static Schema getSchema(String topic, long schemaVersion) {
        return schemas.get(schemaKey(topic, schemaVersion));
    }

    private static String schemaKey(String topic, long schemaVersion) {
        return String.format("%s/%d", topic, schemaVersion);
    }

    private static Schema buildSchema(String schemaDefinition) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        org.apache.avro.Schema parsedSchema = new Parser().parse(schemaDefinition);
        schemaBuilder.name(parsedSchema.getName());
        List<Field> schemaFields = parsedSchema.getFields();
        for (Field field : schemaFields) {
            if (field.schema().getType().equals(Type.UNION)) {
                List<org.apache.avro.Schema> types = field.schema().getTypes();
                for (org.apache.avro.Schema typeOption : types) {
                    Type type = typeOption.getType();
                    if (!type.equals(Type.NULL)) {
                        if (type.equals(Type.RECORD)) {
                            String fieldSchemaDef = typeOption.toString();
                            Schema fieldSchema = buildSchema(fieldSchemaDef);
                            if (field.hasDefaultValue()) {
                                schemaBuilder.field(field.name(), fieldSchema)
                                    .optional().defaultValue(null).build();
                            } else {
                                schemaBuilder.field(field.name(), fieldSchema);
                            }
                        } else if (type.equals(Type.ARRAY)) {
                            org.apache.avro.Schema itemType = typeOption.getElementType();
                            if (itemType.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                                org.apache.avro.Schema kType = itemType.getField(key).schema();
                                Schema keySchema = typeMapping.get(kType.getType());
                                org.apache.avro.Schema vType = itemType.getField(value).schema();
                                Schema valueSchema = typeMapping.get(vType.getType());
                                if (field.hasDefaultValue()) {
                                    schemaBuilder.field(field.name(), SchemaBuilder.map(
                                        keySchema, valueSchema)
                                            .optional()
                                            .defaultValue(null)
                                            .build());
                                } else {
                                    schemaBuilder.field(field.name(), SchemaBuilder.map(
                                        keySchema, valueSchema).build());
                                }
                            } else {
                                Schema valueSchema = typeMapping.get(itemType.getType());
                                if (field.hasDefaultValue()) {
                                    schemaBuilder.field(field.name(), SchemaBuilder.array(
                                        valueSchema)
                                            .optional()
                                            .defaultValue(null)
                                            .build());
                                } else {
                                    schemaBuilder.field(field.name(), SchemaBuilder.array(
                                        valueSchema).build());
                                }
                            }
                        } else if (type.equals(Type.MAP)) {
                            org.apache.avro.Schema valueType = typeOption.getValueType();
                            Schema valueSchema = typeMapping.get(valueType.getType());
                            if (field.hasDefaultValue()) {
                                schemaBuilder.field(field.name(), SchemaBuilder.map(
                                    Schema.STRING_SCHEMA, valueSchema)
                                        .optional()
                                        .defaultValue(null)
                                        .build());
                            } else {
                                schemaBuilder.field(field.name(), SchemaBuilder.map(
                                    Schema.STRING_SCHEMA, valueSchema).build());
                            }
                        } else {
                            Schema fieldSchema = typeMapping.get(type);
                            if (field.hasDefaultValue()) {
                                Object value = defaultValues.get(fieldSchema);
                                schemaBuilder.field(field.name(), SchemaBuilder.type(
                                    fieldSchema.type()).optional().defaultValue(value)
                                    .build());
                            } else {
                                schemaBuilder.field(field.name(), fieldSchema);
                            }
                        }
                        break;
                    }
                }
            } else if (field.schema().getType().equals(Type.ARRAY)) {
                org.apache.avro.Schema itemType = field.schema().getElementType();
                if (itemType.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                    org.apache.avro.Schema kType = itemType.getField(key).schema();
                    Schema keySchema = typeMapping.get(kType.getType());
                    org.apache.avro.Schema vType = itemType.getField(value).schema();
                    Schema valueSchema = typeMapping.get(vType.getType());
                    if (field.hasDefaultValue()) {
                        schemaBuilder.field(field.name(), SchemaBuilder.map(
                            keySchema, valueSchema)
                                .optional()
                                .defaultValue(null)
                                .build());
                    } else {
                        schemaBuilder.field(field.name(), SchemaBuilder.map(
                            keySchema, valueSchema).build());
                    }
                } else {
                    Schema valueSchema = typeMapping.get(itemType.getType());
                    if (field.hasDefaultValue()) {
                        schemaBuilder.field(field.name(), SchemaBuilder.array(
                            valueSchema)
                                .optional()
                                .defaultValue(null)
                                .build());
                    } else {
                        schemaBuilder.field(field.name(), SchemaBuilder.array(
                            valueSchema).build());
                    }
                }
            } else {
                Type type = field.schema().getType();
                Schema fieldSchema = typeMapping.get(type);
                if (field.hasDefaultValue()) {
                    Object value = defaultValues.get(fieldSchema);
                    schemaBuilder.field(field.name(), SchemaBuilder.type(
                        fieldSchema.type()).optional().defaultValue(value)
                        .build());
                } else {
                    schemaBuilder.field(field.name(), fieldSchema);
                }
            }
        }
        return schemaBuilder.build();
    }

    private static String key = "key";
    private static String value = "value";

    private static Map<org.apache.avro.Schema.Type, Schema> typeMapping = new HashMap<>();
    private static Map<Schema, Object> defaultValues = new HashMap<>();

    static {

        typeMapping.put(org.apache.avro.Schema.Type.STRING, Schema.STRING_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.BOOLEAN, Schema.BOOLEAN_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.BYTES, Schema.BYTES_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.INT, Schema.INT32_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.LONG, Schema.INT64_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.FLOAT, Schema.FLOAT32_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.DOUBLE, Schema.FLOAT64_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.ENUM, Schema.STRING_SCHEMA);

        defaultValues.put(Schema.STRING_SCHEMA, null);
        defaultValues.put(Schema.BOOLEAN_SCHEMA, false);
        defaultValues.put(Schema.BYTES_SCHEMA, null);
        defaultValues.put(Schema.INT32_SCHEMA, 0);
        defaultValues.put(Schema.INT64_SCHEMA, 0L);
        defaultValues.put(Schema.FLOAT32_SCHEMA, 0.0F);
        defaultValues.put(Schema.FLOAT64_SCHEMA, 0.0D);

    }

    private SchemaRegistry() {
    }

}
