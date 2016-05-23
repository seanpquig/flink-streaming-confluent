/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.serialization;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * DeserializationSchema that deserializes Avro data from Kafka
 * with the Confluent deserializer.
 */
public class ConfluentAvroDeserializationSchema implements DeserializationSchema<String> {

    private final String schemaRegistryUrl;
    private final int identityMapCapacity;
    private KafkaAvroDecoder kafkaAvroDecoder;

    public ConfluentAvroDeserializationSchema(String schemaRegistyUrl) {
        this(schemaRegistyUrl, 1000);
    }

    public ConfluentAvroDeserializationSchema(String schemaRegistryUrl, int identityMapCapacity) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.identityMapCapacity = identityMapCapacity;
    }

    @Override
    public String deserialize(byte[] message) {
        if (kafkaAvroDecoder == null) {
            SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(this.schemaRegistryUrl, this.identityMapCapacity);
            this.kafkaAvroDecoder = new KafkaAvroDecoder(schemaRegistry);
        }
        return this.kafkaAvroDecoder.fromBytes(message).toString();
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
