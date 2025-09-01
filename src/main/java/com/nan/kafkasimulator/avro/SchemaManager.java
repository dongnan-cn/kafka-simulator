package com.nan.kafkasimulator.avro;

import org.apache.avro.Schema;

import com.nan.kafkasimulator.utils.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Avro Schema管理器，负责管理所有Avro Schema的存储和检索
 */
public class SchemaManager {

    // 单例实例
    private static final SchemaManager INSTANCE = new SchemaManager();

    // Schema存储，key为Schema名称，value为Schema对象
    private final Map<String, Schema> schemaMap = new ConcurrentHashMap<>();

    // Schema版本管理，key为Schema名称，value为版本列表
    private final Map<String, SchemaVersion> schemaVersions = new ConcurrentHashMap<>();

    // Schema ID生成器
    private final AtomicInteger schemaIdGenerator = new AtomicInteger(1);

    // 私有构造函数，确保单例
    private SchemaManager() {
        // 初始化一些示例Schema
        initializeSampleSchemas();
    }

    /**
     * 获取SchemaManager单例实例
     * @return SchemaManager实例
     */
    public static SchemaManager getInstance() {
        return INSTANCE;
    }

    /**
     * 注册新的Schema
     * @param schemaName Schema名称
     * @param schemaJson Schema的JSON表示
     * @return 注册的Schema对象
     * @throws IllegalArgumentException 如果Schema名称已存在或Schema格式无效
     */
    public Schema registerSchema(String schemaName, String schemaJson) {
        if (schemaName == null || schemaName.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema名称不能为空");
        }

        if (schemaJson == null || schemaJson.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema内容不能为空");
        }

        if (schemaMap.containsKey(schemaName)) {
            throw new IllegalArgumentException("Schema名称 '" + schemaName + "' 已存在");
        }

        try {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaJson);

            // 存储Schema
            schemaMap.put(schemaName, schema);

            // 创建Schema版本记录
            int schemaId = schemaIdGenerator.getAndIncrement();
            SchemaVersion version = new SchemaVersion(schemaId, schemaName, schemaJson, 1);
            schemaVersions.put(schemaName, version);

            Logger.log("成功注册Schema: " + schemaName + ", ID: " + schemaId);
            return schema;
        } catch (Exception e) {
            Logger.log("注册Schema失败: " + e.getMessage());
            e.printStackTrace();
            throw new IllegalArgumentException("无效的Schema格式: " + e.getMessage());
        }
    }

    /**
     * 更新现有Schema
     * @param schemaName Schema名称
     * @param schemaJson Schema的JSON表示
     * @return 更新后的Schema对象
     * @throws IllegalArgumentException 如果Schema不存在或Schema格式无效
     */
    public Schema updateSchema(String schemaName, String schemaJson) {
        if (schemaName == null || schemaName.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema名称不能为空");
        }

        if (schemaJson == null || schemaJson.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema内容不能为空");
        }

        if (!schemaMap.containsKey(schemaName)) {
            throw new IllegalArgumentException("Schema名称 '" + schemaName + "' 不存在");
        }

        try {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaJson);

            // 更新Schema
            schemaMap.put(schemaName, schema);

            // 更新Schema版本记录
            SchemaVersion currentVersion = schemaVersions.get(schemaName);
            int newVersion = currentVersion.getVersion() + 1;
            int schemaId = currentVersion.getId();
            SchemaVersion newVersionRecord = new SchemaVersion(schemaId, schemaName, schemaJson, newVersion);
            schemaVersions.put(schemaName, newVersionRecord);

            Logger.log("成功更新Schema: " + schemaName + ", ID: " + schemaId + ", 版本: " + newVersion);
            return schema;
        } catch (Exception e) {
            Logger.log("更新Schema失败: " + e.getMessage());
            e.printStackTrace();
            throw new IllegalArgumentException("无效的Schema格式: " + e.getMessage());
        }
    }

    /**
     * 获取指定名称的最新Schema
     * @param schemaName Schema名称
     * @return Schema对象，如果不存在则返回null
     */
    public Schema getSchema(String schemaName) {
        return schemaMap.get(schemaName);
    }

    /**
     * 获取所有Schema名称
     * @return 不可修改的Schema名称集合
     */
    public Map<String, Schema> getAllSchemas() {
        return Collections.unmodifiableMap(schemaMap);
    }

    /**
     * 获取Schema版本信息
     * @param schemaName Schema名称
     * @return SchemaVersion对象，如果不存在则返回null
     */
    public SchemaVersion getSchemaVersion(String schemaName) {
        return schemaVersions.get(schemaName);
    }

    /**
     * 删除指定名称的Schema
     * @param schemaName Schema名称
     * @return 被删除的Schema对象，如果不存在则返回null
     */
    public Schema removeSchema(String schemaName) {
        Schema removedSchema = schemaMap.remove(schemaName);
        if (removedSchema != null) {
            SchemaVersion removedVersion = schemaVersions.remove(schemaName);
            Logger.log("删除Schema: " + schemaName + ", ID: " +
                     (removedVersion != null ? removedVersion.getId() : "未知"));
        }
        return removedSchema;
    }

    /**
     * 验证Schema格式是否有效
     * @param schemaJson Schema的JSON表示
     * @return 如果有效则返回true，否则返回false
     */
    public boolean validateSchema(String schemaJson) {
        try {
            Schema.Parser parser = new Schema.Parser();
            parser.parse(schemaJson);
            return true;
        } catch (Exception e) {
            Logger.log("Schema验证失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 初始化一些示例Schema
     */
    private void initializeSampleSchemas() {
        // 用户Schema示例
        String userSchema = "{\"type\":\"record\",\"name\":\"User\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"},"
                + "{\"name\":\"name\",\"type\":\"string\"},"
                + "{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

        try {
            registerSchema("User", userSchema);
        } catch (Exception e) {
            Logger.log("初始化示例User Schema失败: " + e.getMessage());
            e.printStackTrace();
        }

        // 产品Schema示例
        String productSchema = "{\"type\":\"record\",\"name\":\"Product\",\"fields\":["
                + "{\"name\":\"productId\",\"type\":\"string\"},"
                + "{\"name\":\"name\",\"type\":\"string\"},"
                + "{\"name\":\"price\",\"type\":\"double\"},"
                + "{\"name\":\"category\",\"type\":\"string\"}]}";

        try {
            registerSchema("Product", productSchema);
        } catch (Exception e) {
            Logger.log("初始化示例Product Schema失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Schema版本信息类
     */
    public static class SchemaVersion {
        private final int id;
        private final String name;
        private final String schemaJson;
        private final int version;

        public SchemaVersion(int id, String name, String schemaJson, int version) {
            this.id = id;
            this.name = name;
            this.schemaJson = schemaJson;
            this.version = version;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getSchemaJson() {
            return schemaJson;
        }

        public int getVersion() {
            return version;
        }

        /**
         * 获取Schema对象
         * @return Schema对象
         */
        public Schema getSchema() {
            try {
                Schema.Parser parser = new Schema.Parser();
                return parser.parse(schemaJson);
            } catch (Exception e) {
                throw new RuntimeException("解析Schema失败: " + e.getMessage(), e);
            }
        }
    }
}