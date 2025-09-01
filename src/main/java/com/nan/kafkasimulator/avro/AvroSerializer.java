package com.nan.kafkasimulator.avro;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Avro序列化器，将JSON格式消息序列化为Avro二进制格式
 */
public class AvroSerializer implements Serializer<String> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SchemaManager schemaManager;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        schemaManager = SchemaManager.getInstance();
    }

    @Override
    public byte[] serialize(String topic, String data) {
        if (data == null) {
            return null;
        }

        try {
            // 检查是否是Avro格式消息
            if (data.startsWith("AVRO:")) {
                // 解析消息格式: AVRO:schemaName:jsonContent
                String[] parts = data.split(":", 3);
                if (parts.length != 3) {
                    throw new IllegalArgumentException("Invalid Avro message format");
                }

                String schemaName = parts[1];
                String jsonContent = parts[2];

                // 获取Schema
                Schema schema = schemaManager.getSchema(schemaName);
                if (schema == null) {
                    throw new IllegalArgumentException("Schema not found: " + schemaName);
                }

                // 将JSON转换为Avro记录
                JsonNode jsonNode = objectMapper.readTree(jsonContent);
                GenericRecord record = JsonToAvroConverter.convertJsonToAvro(jsonNode, schema);

                // 序列化Avro记录
                return serializeRecord(record, schema);
            } else {
                // 非Avro消息，直接返回字节数组
                return data.getBytes();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Avro message", e);
        }
    }

    /**
     * 序列化Avro记录为字节数组
     * @param record Avro记录
     * @param schema Avro Schema
     * @return 序列化后的字节数组
     * @throws IOException 序列化异常
     */
    private byte[] serializeRecord(GenericRecord record, Schema schema) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            writer.write(record, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        }
    }

    @Override
    public void close() {
        // 清理资源
    }
}
