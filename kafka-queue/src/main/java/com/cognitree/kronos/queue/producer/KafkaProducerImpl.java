/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.queue.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

/**
 * A {@link Producer} implementation using Kafka as queue in backend.
 */
public class KafkaProducerImpl implements Producer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String KAFKA_PRODUCER_CONFIG_KEY = "kafkaProducerConfig";
    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final short TOPIC_REPLICATION_FACTOR = (short) 1;

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;
    private ObjectNode config;

    public void init(String topic, ObjectNode config) {
        logger.info("Initializing Kafka producer for topic {} with config {}", topic, config);
        this.topic = topic;
        this.config = config;
        createTopic();
        initProducer();
    }

    private void createTopic() {
        try {
            final Properties properties =
                    OBJECT_MAPPER.convertValue(config.get(KAFKA_PRODUCER_CONFIG_KEY), Properties.class);
            final AdminClient adminClient = AdminClient.create(properties);
            final NewTopic kafkaTopic = new NewTopic(topic, NUM_TOPIC_PARTITIONS, TOPIC_REPLICATION_FACTOR);
            adminClient.createTopics(Collections.singleton(kafkaTopic)).all().get();
        } catch (Exception e) {
            logger.error("Error creating topic {}, error: {}", topic, e.getMessage());
        }
    }

    private void initProducer() {
        final Properties kafkaProducerConfig =
                OBJECT_MAPPER.convertValue(config.get(KAFKA_PRODUCER_CONFIG_KEY), Properties.class);
        kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
    }

    @Override
    public void broadcast(String record) {
        sendInOrder(record, null);
    }

    @Override
    public void send(String record) {
        sendInOrder(record, null);
    }

    @Override
    public void sendInOrder(String record, String orderingKey) {
        logger.trace("Received request to send message {} to topic {} with orderingKey {}",
                record, topic, orderingKey);
        final ProducerRecord<String, String> producerRecord = orderingKey == null ?
                new ProducerRecord<>(topic, record) : new ProducerRecord<>(topic, orderingKey, record);
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending record {} over kafka to topic {}.", record, topic, exception);
            }
        });
    }

    @Override
    public void close() {
        try {
            kafkaProducer.close();
        } catch (Exception e) {
            logger.error("Error closing Kafka producer", e);
        }
    }
}