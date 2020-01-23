/*
 * Copyright © 2019 Muhammad Yousuf (yousuf.undergrad@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.yousufdev.kafka.connect.smt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.config.ConfigDef;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;


public abstract class AppendSchema<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Append schema to a schemaless record otherwise pass the record as it is";

    private interface ConfigName {
        String SCHEMA_FIELD_NAME = "schema";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SCHEMA_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Field name for schema");

    private static final String PURPOSE = "append SCHEMA to record";

    private Schema staticSchema;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        String jsonifySchema = config.getString(ConfigName.SCHEMA_FIELD_NAME).replaceAll("'", "\"");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaObject = null;
        Map<String, String> jsonConverterConfigs = new HashMap<>();
        jsonConverterConfigs.put("converter.type", "key");
        jsonConverterConfigs.put("schemas.enable", "false");

        try {
            schemaObject = mapper.readTree(jsonifySchema);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        JsonConverter jsonConverter= new JsonConverter();
        jsonConverter.configure(jsonConverterConfigs);

        staticSchema = jsonConverter.asConnectSchema(schemaObject.get(ConfigName.SCHEMA_FIELD_NAME));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return record;
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> valueCopy = new HashMap<>(value);

        final Struct updatedValue = new Struct(staticSchema);

        for (String key : valueCopy.keySet()) {
            updatedValue.put(key, valueCopy.get(key));
        }

        return newRecord(record, staticSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends AppendSchema<R> {

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
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends AppendSchema<R> {

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
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}


