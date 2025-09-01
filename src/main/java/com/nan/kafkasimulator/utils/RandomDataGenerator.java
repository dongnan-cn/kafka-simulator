package com.nan.kafkasimulator.utils;

import com.github.javafaker.Faker;
import com.nan.kafkasimulator.avro.SchemaManager;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * 测试数据生成工具类，使用Java Faker生成各种类型的随机数据
 */
public class RandomDataGenerator {
    private static final Faker faker = new Faker(Locale.CHINA); // Use Chinese environment
    private static final Random random = new Random();

    /**
     * 根据Avro Schema生成随机的JSON消息
     * @param schemaName Schema名称
     * @return 符合Schema的随机JSON消息
     */
    public static String generateRandomAvroJson(String schemaName) {
        try {
            SchemaManager.SchemaVersion schemaVersion = SchemaManager.getInstance().getSchemaVersion(schemaName);
            if (schemaVersion == null) {
                Logger.log("Error: Schema not found: " + schemaName);
                return "{}";
            }

            Schema schema = schemaVersion.getSchema();
            return generateRandomJsonForSchema(schema);
        } catch (Exception e) {
            Logger.log("Failed to generate Avro JSON message: " + e.getMessage());
            return "{}";
        }
    }

    /**
     * 根据Avro Schema生成随机的JSON消息
     * @param schema Avro Schema
     * @return 符合Schema的随机JSON消息
     */
    public static String generateRandomJsonForSchema(Schema schema) {
        StringBuilder sb = new StringBuilder("{");
        List<Schema.Field> fields = schema.getFields();

        for (int i = 0; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);
            String fieldName = field.name();
            Schema fieldSchema = field.schema();

            sb.append(String.format("%s : %s", fieldName, generateRandomValueForSchema(fieldSchema, fieldName)));

            if (i < fields.size() - 1) {
                sb.append(", ");
            }
        }

        sb.append("}");
        return sb.toString();
    }

    /**
     * 根据Avro Schema类型生成随机值
     * @param schema Avro Schema
     * @return 随机值的JSON表示
     */
    public static String generateRandomValueForSchema(Schema schema) {
        return generateRandomValueForSchema(schema, null);
    }

    /**
     * 根据Avro Schema类型生成随机值
     * @param schema Avro Schema
     * @param fieldName 字段名称（用于生成更合适的数据）
     * @return 随机值的JSON表示
     */
    public static String generateRandomValueForSchema(Schema schema, String fieldName) {
        switch (schema.getType()) {
            case STRING:
                return generateRandomString(fieldName);
            case INT:
                return String.valueOf(random.nextInt(1000));
            case LONG:
                return String.valueOf(random.nextLong() % 10000);
            case FLOAT:
                return String.valueOf(random.nextFloat() * 1000);
            case DOUBLE:
                return String.valueOf(random.nextDouble() * 10000);
            case BOOLEAN:
                return random.nextBoolean() ? "true" : "false";
            case NULL:
                return "null";
            case ARRAY:
                return generateRandomArray(schema.getElementType());
            case MAP:
                return generateRandomMap(schema.getValueType());
            case RECORD:
                return generateRandomJsonForSchema(schema);
            case UNION:
                // For union types, prefer non-null types
                List<Schema> types = schema.getTypes();
                if (!types.isEmpty()) {
                    // If there's only one type, use it directly
                    if (types.size() == 1) {
                        return generateRandomValueForSchema(types.get(0), fieldName);
                    }

                    // If there are multiple types, prefer non-null types
                    for (Schema type : types) {
                        if (type.getType() != Schema.Type.NULL) {
                            return generateRandomValueForSchema(type, fieldName);
                        }
                    }

                    // If only null type, return null
                    return "null";
                }
                return "null";
            default:
                return "null";
        }
    }

    /**
     * 生成随机数组
     * @param elementSchema 元素类型
     * @return 随机数组的JSON表示
     */
    public static String generateRandomArray(Schema elementSchema) {
        int size = random.nextInt(5) + 1; // 1-5 elements
        StringBuilder sb = new StringBuilder("[");

        for (int i = 0; i < size; i++) {
            sb.append(generateRandomValueForSchema(elementSchema));
            if (i < size - 1) {
                sb.append(", ");
            }
        }

        sb.append("]");
        return sb.toString();
    }

    /**
     * 生成随机Map
     * @param valueSchema 值类型
     * @return 随机Map的JSON表示
     */
    public static String generateRandomMap(Schema valueSchema) {
        int size = random.nextInt(3) + 1; // 1-3 key-value pairs
        StringBuilder sb = new StringBuilder("{");

        for (int i = 0; i < size; i++) {
            sb.append(String.format("key %d : %s", i, generateRandomValueForSchema(valueSchema)));
            if (i < size - 1) {
                sb.append(", ");
            }
        }

        sb.append("}");
        return sb.toString();
    }

    /**
     * 生成随机字符串
     * @return 随机字符串
     */
    public static String generateRandomString() {
        return generateRandomString(null);
    }

    /**
     * 生成随机字符串
     * @param fieldName 字段名称（用于生成更合适的数据）
     * @return 随机字符串
     */
    public static String generateRandomString(String fieldName) {
        // 如果指定了字段名称，根据名称生成更合适的数据
        if (fieldName != null) {
            String lowerFieldName = fieldName.toLowerCase();

            // 根据字段名称判断应该生成什么类型的数据
            if (lowerFieldName.contains("email") || lowerFieldName.contains("mail")) {
                // 使用英文环境生成电子邮件地址，避免中文
                Faker englishFaker = new Faker(Locale.ENGLISH);
                return englishFaker.internet().emailAddress();
            } else if (lowerFieldName.contains("name")) {
                return faker.name().fullName();
            } else if (lowerFieldName.contains("phone") || lowerFieldName.contains("mobile") || lowerFieldName.contains("telephone")) {
                return faker.phoneNumber().cellPhone();
            } else if (lowerFieldName.contains("address")) {
                return faker.address().fullAddress();
            } else if (lowerFieldName.contains("city")) {
                return faker.address().city();
            } else if (lowerFieldName.contains("country")) {
                return faker.address().country();
            } else if (lowerFieldName.contains("company") || lowerFieldName.contains("corp") || lowerFieldName.contains("org")) {
                return faker.company().name();
            } else if (lowerFieldName.contains("id")) {
                return String.valueOf(random.nextInt(1000000));
            } else if (lowerFieldName.contains("url") || lowerFieldName.contains("website") || lowerFieldName.contains("homepage")) {
                return faker.internet().url();
            } else if (lowerFieldName.contains("username") || lowerFieldName.contains("user")) {
                return faker.name().username();
            } else if (lowerFieldName.contains("password") || lowerFieldName.contains("pwd")) {
                return faker.internet().password();
            }
        }

        // 如果没有指定字段名称或无法匹配，则随机生成一种类型的数据
        int choice = random.nextInt(6);
        switch (choice) {
            case 0:
                return faker.name().fullName();
            case 1:
                // 使用英文环境生成电子邮件地址，避免中文
                Faker englishFaker = new Faker(Locale.ENGLISH);
                return englishFaker.internet().emailAddress();
            case 2:
                return faker.address().city();
            case 3:
                return faker.company().name();
            case 4:
                return faker.lorem().word();
            case 5:
                return faker.phoneNumber().cellPhone();
            default:
                return faker.lorem().characters(10);
        }
    }

    /**
     * 生成指定长度的随机字符串
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String generateRandomString(int length) {
        return faker.lorem().characters(length);
    }

    /**
     * 生成随机JSON消息
     * @param fieldCount 字段数量
     * @return 随机JSON消息
     */
    public static String generateRandomJson(int fieldCount) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fieldCount; i++) {
            sb.append(String.format("key %d : %s", i, generateRandomString(8)));
            if (i < fieldCount - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * 生成随机的电子邮件地址
     * @return 随机电子邮件地址
     */
    public static String generateRandomEmail() {
        // 使用英文环境生成电子邮件地址，避免中文
        Faker englishFaker = new Faker(Locale.ENGLISH);
        return englishFaker.internet().emailAddress();
    }

    /**
     * 生成随机的姓名
     * @return 随机姓名
     */
    public static String generateRandomName() {
        return faker.name().fullName();
    }

    /**
     * 生成随机的地址
     * @return 随机地址
     */
    public static String generateRandomAddress() {
        return faker.address().fullAddress();
    }

    /**
     * 生成随机的公司名称
     * @return 随机公司名称
     */
    public static String generateRandomCompany() {
        return faker.company().name();
    }

    /**
     * 生成随机的句子
     * @return 随机句子
     */
    public static String generateRandomSentence() {
        return faker.lorem().sentence();
    }

    /**
     * 生成随机的数字
     * @param min 最小值
     * @param max 最大值
     * @return 随机数字
     */
    public static int generateRandomNumber(int min, int max) {
        return faker.number().numberBetween(min, max);
    }
}
