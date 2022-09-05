package com.devgurupk.kafka.connect.transform;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class SnowflakeRecordSMT<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String INTEGRITY_FIELD_CONFIG = "field";
    private static final String INTEGRITY_FIELD_NAME = "integrity";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(INTEGRITY_FIELD_CONFIG, ConfigDef.Type.STRING,
                    INTEGRITY_FIELD_NAME, ConfigDef.Importance.MEDIUM,
                    "Integrity field name to add.");

    private String fieldName;

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(INTEGRITY_FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
        System.out.println("In apply() \n\n");
        System.out.println(record);
        System.out.println("\n");

        System.out.println(record.keySchema());
        System.out.println("\n");
        System.out.println(record.valueSchema());
        System.out.println("\n");
        System.out.println(record.key());
        System.out.println("\n");
        System.out.println(record.value());

        Object obj= record.value();

        SnowFlakeRecord.processTheRecord(record.value());

        System.out.println("End apply() \n\n");

        return insertIntegrityWithoutSchema(record);
    }

    private static final String PURPOSE = "integrity field addition";

    private R insertIntegrityWithoutSchema(R record) {
        System.out.println(SnowFlakeRecord.processTheRecord(record.value()));
        Map<String, Object> value = SnowFlakeRecord.processTheRecord(record.value());
        System.out.println(value);
        return newRecord(record, null, value);
    }

    private R insertIntegrityWithSchema(R record) {
        Schema schema = operatingSchema(record);

        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        Schema updatedSchema = builder.field(this.fieldName, SchemaBuilder.STRING_SCHEMA)
                .build();

        Struct value = requireStructOrNull(operatingValue(record), PURPOSE);

        String stringToHash = value.schema()
                .fields()
                .stream()
                .map(Field::toString)
                .collect(Collectors.joining());
        String sha = DigestUtils.sha256Hex(stringToHash);

        Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(fieldName, sha);

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends SnowflakeRecordSMT<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema,
                    updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends SnowflakeRecordSMT<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            System.out.println(updatedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
                    record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}
