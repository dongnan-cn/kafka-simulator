package com.nan.kafkasimulator.avro;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Avro反序列化器，将Avro二进制格式消息反序列化为JSON格式
 */
public class AvroDeserializer implements Deserializer<String> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SchemaManager schemaManager;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        schemaManager = SchemaManager.getInstance();
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            // 尝试解析为Avro消息
            // 这里假设消息格式为: [schema_id_length][schema_id][avro_data]
            if (data.length < 4) {
                // 不是Avro格式，直接返回字符串
                return new String(data);
            }

            // 读取schema ID长度（4字节）
            int schemaIdLength = ((data[0] & 0xFF) << 24) | 
                                 ((data[1] & 0xFF) << 16) | 
                                 ((data[2] & 0xFF) << 8) | 
                                 (data[3] & 0xFF);

            if (data.length < 4 + schemaIdLength) {
                // 不是Avro格式，直接返回字符串
                return new String(data);
            }

            // 读取schema ID
            byte[] schemaIdBytes = new byte[schemaIdLength];
            System.arraycopy(data, 4, schemaIdBytes, 0, schemaIdLength);
            String schemaId = new String(schemaIdBytes);

            // 获取Schema
            Schema schema = schemaManager.getSchema(schemaId);
            if (schema == null) {
                throw new IllegalArgumentException("Schema not found: " + schemaId);
            }

            // 读取Avro数据
            byte[] avroData = new byte[data.length - 4 - schemaIdLength];
            System.arraycopy(data, 4 + schemaIdLength, avroData, 0, avroData.length);

            // 反序列化Avro记录
            GenericRecord record = deserializeRecord(avroData, schema);

            // 将Avro记录转换为JSON
            ObjectNode jsonNode = AvroToJsonConverter.convertAvroToJson(record);

            // 返回格式化的JSON字符串
            return objectMapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            // 如果反序列化失败，尝试作为普通字符串处理
            try {
                return new String(data);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to deserialize message", e);
            }
        }
    }

    /**
     * 反序列化字节数组为Avro记录
     * @param data 字节数组
     * @param schema Avro Schema
     * @return Avro记录
     * @throws IOException 反序列化异常
     */
    private GenericRecord deserializeRecord(byte[] data, Schema schema) throws IOException {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            return reader.read(null, decoder);
        }
    }

    @Override
    public void close() {
        // 清理资源
    }
}
