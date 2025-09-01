package com.nan.kafkasimulator.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.Map;

/**
 * JSON到Avro的转换器
 */
public class JsonToAvroConverter {

    /**
     * 将JSON节点转换为Avro记录
     * @param jsonNode JSON节点
     * @param schema Avro Schema
     * @return Avro记录
     */
    public static GenericRecord convertJsonToAvro(JsonNode jsonNode, Schema schema) {
        if (jsonNode == null || !jsonNode.isObject()) {
            throw new IllegalArgumentException("JSON node must be an object");
        }

        GenericRecord record = new GenericData.Record(schema);
        ObjectNode jsonObject = (ObjectNode) jsonNode;
        Iterator<Map.Entry<String, JsonNode>> fields = jsonObject.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();

            Schema.Field schemaField = schema.getField(fieldName);
            if (schemaField == null) {
                // 忽略Schema中不存在的字段
                continue;
            }

            Object value = convertJsonNodeToAvro(fieldValue, schemaField.schema());
            record.put(fieldName, value);
        }

        return record;
    }

    /**
     * 将JSON节点转换为Avro值
     * @param jsonNode JSON节点
     * @param schema Avro Schema
     * @return Avro值
     */
    private static Object convertJsonNodeToAvro(JsonNode jsonNode, Schema schema) {
        if (jsonNode == null || jsonNode.isNull()) {
            return null;
        }

        switch (schema.getType()) {
            case RECORD:
                if (!jsonNode.isObject()) {
                    throw new IllegalArgumentException("Expected JSON object for RECORD type");
                }
                return convertJsonToAvro(jsonNode, schema);

            case ARRAY:
                if (!jsonNode.isArray()) {
                    throw new IllegalArgumentException("Expected JSON array for ARRAY type");
                }
                ArrayNode arrayNode = (ArrayNode) jsonNode;
                Object[] array = new Object[arrayNode.size()];
                for (int i = 0; i < arrayNode.size(); i++) {
                    array[i] = convertJsonNodeToAvro(arrayNode.get(i), schema.getElementType());
                }
                return array;

            case MAP:
                if (!jsonNode.isObject()) {
                    throw new IllegalArgumentException("Expected JSON object for MAP type");
                }
                ObjectNode mapNode = (ObjectNode) jsonNode;
                Map<String, Object> map = new java.util.HashMap<>();
                Iterator<Map.Entry<String, JsonNode>> fields = mapNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    map.put(field.getKey(), convertJsonNodeToAvro(field.getValue(), schema.getValueType()));
                }
                return map;

            case UNION:
                // 对于联合类型，尝试每个可能的类型，直到找到匹配的
                for (Schema possibleSchema : schema.getTypes()) {
                    try {
                        return convertJsonNodeToAvro(jsonNode, possibleSchema);
                    } catch (IllegalArgumentException e) {
                        // 继续尝试下一个类型
                    }
                }
                throw new IllegalArgumentException("No matching type found in UNION for JSON value: " + jsonNode);

            case STRING:
                if (!jsonNode.isTextual()) {
                    throw new IllegalArgumentException("Expected JSON string for STRING type");
                }
                return jsonNode.asText();

            case BYTES:
                if (!jsonNode.isTextual()) {
                    throw new IllegalArgumentException("Expected JSON string for BYTES type");
                }
                return jsonNode.asText().getBytes();

            case INT:
                if (!jsonNode.isInt()) {
                    throw new IllegalArgumentException("Expected JSON integer for INT type");
                }
                return jsonNode.asInt();

            case LONG:
                if (!jsonNode.isLong()) {
                    throw new IllegalArgumentException("Expected JSON long for LONG type");
                }
                return jsonNode.asLong();

            case FLOAT:
                if (!jsonNode.isDouble()) {
                    throw new IllegalArgumentException("Expected JSON double for FLOAT type");
                }
                return (float) jsonNode.asDouble();

            case DOUBLE:
                if (!jsonNode.isDouble()) {
                    throw new IllegalArgumentException("Expected JSON double for DOUBLE type");
                }
                return jsonNode.asDouble();

            case BOOLEAN:
                if (!jsonNode.isBoolean()) {
                    throw new IllegalArgumentException("Expected JSON boolean for BOOLEAN type");
                }
                return jsonNode.asBoolean();

            case NULL:
                return null;

            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + schema.getType());
        }
    }
}
