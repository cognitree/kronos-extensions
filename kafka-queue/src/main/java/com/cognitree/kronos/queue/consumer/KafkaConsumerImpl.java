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

package com.cognitree.kronos.queue.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * A {@link Consumer} implementation using Kafka as queue in backend.
 */
public class KafkaConsumerImpl implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String GROUP_ID = "group.id";
    private static final String CONSUMER_KEY = "consumerKey";
    private static final String MAX_POLL_RECORDS = "max.poll.records";
    private static final String POLL_TIMEOUT_IN_MS = "pollTimeoutInMs";
    private static final String KAFKA_CONSUMER_CONFIG = "kafkaConsumerConfig";
    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final short TOPIC_REPLICATION_FACTOR = (short) 1;

    private long pollTimeoutInMs;
    private String topic;
    private KafkaConsumer<String, String> kafkaConsumer;
    private ObjectNode config;

    public void init(String topic, ObjectNode config) {
        logger.info("Initializing Kafka consumer on topic {} with config {}", topic, config);
        this.topic = topic;
        this.config = config;
        pollTimeoutInMs = config.get(POLL_TIMEOUT_IN_MS).asLong();
        createTopic();
        initConsumer();
    }

    private void createTopic() {
        try {
            final Properties properties =
                    OBJECT_MAPPER.convertValue(config.get(KAFKA_CONSUMER_CONFIG), Properties.class);
            final AdminClient adminClient = AdminClient.create(properties);
            final NewTopic kafkaTopic = new NewTopic(topic, NUM_TOPIC_PARTITIONS, TOPIC_REPLICATION_FACTOR);
            adminClient.createTopics(Collections.singleton(kafkaTopic)).all().get();
        } catch (Exception e) {
            logger.warn("Error creating topic {}, error: {}", topic, e.getMessage());
        }
    }

    private void initConsumer() {
        final Properties kafkaConsumerConfig =
                OBJECT_MAPPER.convertValue(config.get(KAFKA_CONSUMER_CONFIG), Properties.class);
        // force override consumer configuration for kafka to poll max 1 message at a time
        kafkaConsumerConfig.put(MAX_POLL_RECORDS, 1);
        kafkaConsumerConfig.put(GROUP_ID, config.get(CONSUMER_KEY).asText());
        kafkaConsumer = new KafkaConsumer<>(kafkaConsumerConfig);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public List<String> poll() {
        return poll(Integer.MAX_VALUE);
    }

    @Override
    public synchronized List<String> poll(int size) {
        logger.trace("Received request to poll messages from topic {} with max size {}", topic, size);
        List<String> tasks = new ArrayList<>();

        while (tasks.size() < size) {
            final ConsumerRecords<String, String> consumerRecords = kafkaConsumer
                    .poll(Duration.ofMillis(pollTimeoutInMs));
            if (consumerRecords.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                tasks.add(consumerRecord.value());
            }
        }
        return tasks;
    }

    @Override
    public synchronized void close() {
        try {
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing Kafka consumer for topic {}", topic, e);
        }
    }

    @Override
    public synchronized void destroy() {
        logger.info("Received request to destroy consumer and topic {}", topic);
        final Properties properties =
                OBJECT_MAPPER.convertValue(config.get(KAFKA_CONSUMER_CONFIG), Properties.class);
        final AdminClient adminClient = AdminClient.create(properties);

        String consumerGroupKey = config.get(CONSUMER_KEY).asText();
        logger.info("Deleting Kafka consumer group {} and topic {}", consumerGroupKey, topic);
        try {
            adminClient.deleteConsumerGroups(Collections.singletonList(consumerGroupKey));
        } catch (Exception e) {
            logger.warn("Error deleting Kafka consumer group {}", consumerGroupKey, e);
        }
        try {
            adminClient.deleteTopics(Collections.singleton(topic));
        } catch (Exception e) {
            logger.warn("Error deleting Kafka topic {}", topic, e);
        }
        try {
            adminClient.close();
        } catch (Exception e) {
            logger.warn("Error closing Kafka admin client", e);
        }
    }
}