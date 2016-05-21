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

package org.apache.flink.streaming.util.serialization

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

/**
  * DeserializationSchema that deserializes Avro data from Kafka
  * with the Confluent deserializer.
  */
class ConfluentAvroDeserializationSchema(schemaRegistyUrl: String, identityMapCapacity: Int = 1000)
    extends DeserializationSchema[String] {

  private lazy val schemaRegistry = new CachedSchemaRegistryClient(schemaRegistyUrl, identityMapCapacity)
  private lazy val kafkaAvroDecoder = new KafkaAvroDecoder(schemaRegistry)

  override def deserialize(message: Array[Byte]): String = {
    kafkaAvroDecoder.fromBytes(message).toString
  }

  override def isEndOfStream(nextElement: String): Boolean = false

  override def getProducedType(): TypeInformation[String] = BasicTypeInfo.STRING_TYPE_INFO

}
