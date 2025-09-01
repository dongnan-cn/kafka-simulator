package com.nan.kafkasimulator.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

/**
 * Avro到JSON的转换器
 */
public class AvroToJsonConverter {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 将Avro记录转换为JSON对象
     * @param record Avro记录
     * @return JSON对象节点
     */
    public static ObjectNode convertAvroToJson(GenericRecord record) {
        ObjectNode jsonObject = objectMapper.createObjectNode();
        Schema schema = record.getSchema();

        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object fieldValue = record.get(fieldName);
            Schema fieldSchema = field.schema();

            jsonObject.set(fieldName, convertAvroValueToJson(fieldValue, fieldSchema));
        }

        return jsonObject;
    }

    /**
     * 将Avro值转换为JSON节点
     * @param value Avro值
     * @param schema Avro Schema
     * @return JSON节点
     */
    private static com.fasterxml.jackson.databind.JsonNode convertAvroValueToJson(Object value, Schema schema) {
        if (value == null) {
            return NullNode.getInstance();
        }

        switch (schema.getType()) {
            case RECORD:
                if (!(value instanceof GenericRecord)) {
                    throw new IllegalArgumentException("Expected GenericRecord for RECORD type");
                }
                return convertAvroToJson((GenericRecord) value);

            case ARRAY:
                if (!(value instanceof List)) {
                    throw new IllegalArgumentException("Expected List for ARRAY type");
                }
                ArrayNode arrayNode = objectMapper.createArrayNode();
                List<?> list = (List<?>) value;
                for (Object item : list) {
                    arrayNode.add(convertAvroValueToJson(item, schema.getElementType()));
                }
                return arrayNode;

            case MAP:
                if (!(value instanceof Map)) {
                    throw new IllegalArgumentException("Expected Map for MAP type");
                }
                ObjectNode mapNode = objectMapper.createObjectNode();
                Map<?, ?> map = (Map<?, ?>) value;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String)) {
                        throw new IllegalArgumentException("Map keys must be strings");
                    }
                    mapNode.set((String) entry.getKey(), convertAvroValueToJson(entry.getValue(), schema.getValueType()));
                }
                return mapNode;

            case UNION:
                // 对于联合类型，确定实际类型并转换
                for (Schema possibleSchema : schema.getTypes()) {
                    try {
                        return convertAvroValueToJson(value, possibleSchema);
                    } catch (IllegalArgumentException e) {
                        // 继续尝试下一个类型
                    }
                }
                throw new IllegalArgumentException("No matching type found in UNION for Avro value: " + value);

            case STRING:
                if (!(value instanceof CharSequence)) {
                    throw new IllegalArgumentException("Expected CharSequence for STRING type");
                }
                return new TextNode(value.toString());

            case BYTES:
                if (!(value instanceof byte[])) {
                    throw new IllegalArgumentException("Expected byte[] for BYTES type");
                }
                return new TextNode(new String((byte[]) value));

            case INT:
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("Expected Integer for INT type");
                }
                return new IntNode((Integer) value);

            case LONG:
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException("Expected Long for LONG type");
                }
                return new LongNode((Long) value);

            case FLOAT:
                if (!(value instanceof Float)) {
                    throw new IllegalArgumentException("Expected Float for FLOAT type");
                }
                return new DoubleNode((Float) value);

            case DOUBLE:
                if (!(value instanceof Double)) {
                    throw new IllegalArgumentException("Expected Double for DOUBLE type");
                }
                return new DoubleNode((Double) value);

            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException("Expected Boolean for BOOLEAN type");
                }
                return (Boolean) value ? BooleanNode.TRUE : BooleanNode.FALSE;

            case NULL:
                return NullNode.getInstance();

            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + schema.getType());
        }
    }
}
