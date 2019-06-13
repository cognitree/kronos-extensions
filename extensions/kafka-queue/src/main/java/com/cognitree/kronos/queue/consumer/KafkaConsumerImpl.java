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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link Consumer} implementation using Kafka as queue in backend.
 */
public class KafkaConsumerImpl implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String GROUP_ID = "group.id";

    private final Map<String, KafkaConsumer<String, String>> topicToKafkaConsumerMap = new HashMap<>();
    private Properties kafkaConsumerConfig;
    private long pollTimeoutInMs;


    public void init(ObjectNode config) {
        logger.info("Initializing consumer for kafka with config {}", config);
        kafkaConsumerConfig = OBJECT_MAPPER.convertValue(config.get("kafkaConsumerConfig"), Properties.class);
        // force override consumer configuration for kafka to poll max 1 message at a time
        kafkaConsumerConfig.put("max.poll.records", 1);
        pollTimeoutInMs = config.get("pollTimeoutInMs").asLong();
    }

    @Override
    public List<String> poll(String topic) {
        return poll(topic, Integer.MAX_VALUE);
    }

    @Override
    public List<String> poll(String topic, int size) {
        logger.trace("Received request to poll messages from topic {} with max size {}", topic, size);
        List<String> tasks = new ArrayList<>();
        if (!topicToKafkaConsumerMap.containsKey(topic)) {
            createKafkaConsumer(topic);
        }

        final KafkaConsumer<String, String> kafkaConsumer = topicToKafkaConsumerMap.get(topic);
        synchronized (kafkaConsumer) {
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
        }

        return tasks;
    }

    private synchronized void createKafkaConsumer(String topic) {
        if (!topicToKafkaConsumerMap.containsKey(topic)) {
            final Properties kafkaConsumerConfig = new Properties();
            kafkaConsumerConfig.putAll(this.kafkaConsumerConfig);
            kafkaConsumerConfig.put(GROUP_ID, kafkaConsumerConfig.getProperty(GROUP_ID) + "-" + topic);
            logger.info("Creating kafka consumer on topic {} with consumer config {}", topic, kafkaConsumerConfig);
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConsumerConfig);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            topicToKafkaConsumerMap.put(topic, kafkaConsumer);
        }
    }

    @Override
    public void close() {
        topicToKafkaConsumerMap.forEach((topic, kafkaConsumer) -> {
            synchronized (kafkaConsumer) {
                try {
                    kafkaConsumer.close();
                } catch (Exception e) {
                    logger.warn("Error closing Kafka consumer for topic {}", topic, e);
                }
            }
        });
    }

    @Override
    public void destroy() {
        logger.info("Received request to delete created topics and consumer groups from Kafka");
        AdminClient adminClient = AdminClient.create(kafkaConsumerConfig);
        Set<String> topics = topicToKafkaConsumerMap.keySet();
        Set<String> consumerGroups =
                topics.stream().map(topic -> kafkaConsumerConfig.getProperty(GROUP_ID) + "-" + topic)
                        .collect(Collectors.toSet());
        logger.info("Deleting Kafka consumer group {} and topics {}", consumerGroups, topics);
        try {
            adminClient.deleteConsumerGroups(consumerGroups);
        } catch (Exception e) {
            logger.warn("Error deleting Kafka consumer groups {}", consumerGroups, e);
        }
        try {
            adminClient.deleteTopics(topics);
        } catch (Exception e) {
            logger.warn("Error deleting Kafka topics {}", topics, e);
        }
        try {
            adminClient.close();
        } catch (Exception e) {
            logger.warn("Error closing Kafka admin client", e);
        }
    }
}