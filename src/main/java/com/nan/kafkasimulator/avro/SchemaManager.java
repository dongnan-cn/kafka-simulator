package com.nan.kafkasimulator.avro;

import org.apache.avro.Schema;

import com.nan.kafkasimulator.utils.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Avro Schema manager, responsible for managing storage and retrieval of all Avro Schemas
 */
public class SchemaManager {

    // Singleton instance
    private static final SchemaManager INSTANCE = new SchemaManager();

    // Schema storage, key is schema name, value is schema object
    private final Map<String, Schema> schemaMap = new ConcurrentHashMap<>();

    // Schema version management, key is schema name, value is version list
    private final Map<String, SchemaVersion> schemaVersions = new ConcurrentHashMap<>();

    // Schema ID generator
    private final AtomicInteger schemaIdGenerator = new AtomicInteger(1);

    // Private constructor to ensure singleton
    private SchemaManager() {
        // Initialize some sample schemas
        initializeSampleSchemas();
    }

    /**
     * Get SchemaManager singleton instance
     * @return SchemaManager instance
     */
    public static SchemaManager getInstance() {
        return INSTANCE;
    }

    /**
     * Register new Schema
     * @param schemaName Schema name
     * @param schemaJson JSON representation of Schema
     * @return Registered Schema object
     * @throws IllegalArgumentException If schema name already exists or schema format is invalid
     */
    public Schema registerSchema(String schemaName, String schemaJson) {
        if (schemaName == null || schemaName.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema name cannot be empty");
        }

        if (schemaJson == null || schemaJson.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema content cannot be empty");
        }

        if (schemaMap.containsKey(schemaName)) {
            throw new IllegalArgumentException("Schema name '" + schemaName + "' already exists");
        }

        try {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaJson);

            // Store Schema
            schemaMap.put(schemaName, schema);

            // Create Schema version record
            int schemaId = schemaIdGenerator.getAndIncrement();
            SchemaVersion version = new SchemaVersion(schemaId, schemaName, schemaJson, 1);
            schemaVersions.put(schemaName, version);

            Logger.log("Successfully registered Schema: " + schemaName + ", ID: " + schemaId);
            return schema;
        } catch (Exception e) {
            Logger.log("Failed to register Schema: " + e.getMessage());
            e.printStackTrace();
            throw new IllegalArgumentException("Invalid Schema format: " + e.getMessage());
        }
    }

    /**
     * Update existing Schema
     * @param schemaName Schema name
     * @param schemaJson JSON representation of Schema
     * @return Updated Schema object
     * @throws IllegalArgumentException If schema does not exist or schema format is invalid
     */
    public Schema updateSchema(String schemaName, String schemaJson) {
        if (schemaName == null || schemaName.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema name cannot be empty");
        }

        if (schemaJson == null || schemaJson.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema content cannot be empty");
        }

        if (!schemaMap.containsKey(schemaName)) {
            throw new IllegalArgumentException("Schema name '" + schemaName + "' does not exist");
        }

        try {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaJson);

            // Update Schema
            schemaMap.put(schemaName, schema);

            // Update Schema version record
            SchemaVersion currentVersion = schemaVersions.get(schemaName);
            int newVersion = currentVersion.getVersion() + 1;
            int schemaId = currentVersion.getId();
            SchemaVersion newVersionRecord = new SchemaVersion(schemaId, schemaName, schemaJson, newVersion);
            schemaVersions.put(schemaName, newVersionRecord);

            Logger.log("Successfully updated Schema: " + schemaName + ", ID: " + schemaId + ", Version: " + newVersion);
            return schema;
        } catch (Exception e) {
            Logger.log("Failed to update Schema: " + e.getMessage());
            e.printStackTrace();
            throw new IllegalArgumentException("Invalid Schema format: " + e.getMessage());
        }
    }

    /**
     * Get the latest Schema with specified name
     * @param schemaName Schema name
     * @return Schema object, returns null if not exists
     */
    public Schema getSchema(String schemaName) {
        return schemaMap.get(schemaName);
    }

    /**
     * Get all Schema names
     * @return Unmodifiable collection of Schema names
     */
    public Map<String, Schema> getAllSchemas() {
        return Collections.unmodifiableMap(schemaMap);
    }

    /**
     * Get Schema version information
     * @param schemaName Schema name
     * @return SchemaVersion object, returns null if not exists
     */
    public SchemaVersion getSchemaVersion(String schemaName) {
        return schemaVersions.get(schemaName);
    }

    /**
     * Delete Schema with specified name
     * @param schemaName Schema name
     * @return Deleted Schema object, returns null if not exists
     */
    public Schema removeSchema(String schemaName) {
        Schema removedSchema = schemaMap.remove(schemaName);
        if (removedSchema != null) {
            SchemaVersion removedVersion = schemaVersions.remove(schemaName);
            Logger.log("Deleted Schema: " + schemaName + ", ID: " +
                     (removedVersion != null ? removedVersion.getId() : "Unknown"));
        }
        return removedSchema;
    }

    /**
     * Validate if Schema format is valid
     * @param schemaJson JSON representation of Schema
     * @return Returns true if valid, otherwise returns false
     */
    public boolean validateSchema(String schemaJson) {
        try {
            Schema.Parser parser = new Schema.Parser();
            parser.parse(schemaJson);
            return true;
        } catch (Exception e) {
            Logger.log("Schema validation failed: " + e.getMessage());
            return false;
        }
    }

    /**
     * Initialize some sample schemas
     */
    private void initializeSampleSchemas() {
        // User Schema example
        String userSchema = "{\"type\":\"record\",\"name\":\"User\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"},"
                + "{\"name\":\"name\",\"type\":\"string\"},"
                + "{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

        try {
            registerSchema("User", userSchema);
        } catch (Exception e) {
            Logger.log("Failed to initialize sample User Schema: " + e.getMessage());
            e.printStackTrace();
        }

        // Product Schema example
        String productSchema = "{\"type\":\"record\",\"name\":\"Product\",\"fields\":["
                + "{\"name\":\"productId\",\"type\":\"string\"},"
                + "{\"name\":\"name\",\"type\":\"string\"},"
                + "{\"name\":\"price\",\"type\":\"double\"},"
                + "{\"name\":\"category\",\"type\":\"string\"}]}";

        try {
            registerSchema("Product", productSchema);
        } catch (Exception e) {
            Logger.log("Failed to initialize sample Product Schema: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Schema version information class
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
         * Get Schema object
         * @return Schema object
         */
        public Schema getSchema() {
            try {
                Schema.Parser parser = new Schema.Parser();
                return parser.parse(schemaJson);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse Schema: " + e.getMessage(), e);
            }
        }
    }
}