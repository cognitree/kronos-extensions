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

package com.cognitree.kronos.executor.handlers;

import com.cognitree.kronos.executor.model.TaskResult;
import com.cognitree.kronos.model.Task;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * A {@link TaskHandler} implementation to push a message to a kafka topic.
 */
public class KafkaMessageHandler implements TaskHandler {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageHandler.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String KAFKA_PRODUCER_CONFIG_KEY = "kafkaProducerConfig";
    // topic to push the message
    private static final String TOPIC_KEY = "topic";
    // message to push to kafka topic
    private static final String MESSAGE_KEY = "message";

    private JsonNode producerConfig;
    private String defaultTopic;
    private Task task;

    @Override
    public void init(Task task, ObjectNode config) {
        if (config == null || !config.hasNonNull(KAFKA_PRODUCER_CONFIG_KEY)) {
            throw new IllegalArgumentException("missing mandatory configuration: [kafkaProducerConfig]");
        }
        this.task = task;
        producerConfig = config.get(KAFKA_PRODUCER_CONFIG_KEY);
        defaultTopic = config.get(TOPIC_KEY).asText();
    }

    @Override
    public TaskResult execute() {
        logger.info("Received request to execute task {}", task);
        final Map<String, Object> taskProperties = task.getProperties();
        final String topic = (String) taskProperties.getOrDefault(TOPIC_KEY, defaultTopic);
        try {
            final Object message = taskProperties.get(MESSAGE_KEY);
            send(topic, OBJECT_MAPPER.writeValueAsString(message));
            return TaskResult.SUCCESS;
        } catch (JsonProcessingException e) {
            logger.error("error parsing task properties {}", taskProperties, e);
            return new TaskResult(false, "Error parsing task properties : " + e.getMessage());
        }
    }

    private void send(String topic, String record) {
        logger.debug("Received request to send message {} to topic {}.", record, topic);
        logger.debug("Initializing producer for kafka with config {}", producerConfig);
        final Properties kafkaProducerConfig = OBJECT_MAPPER.convertValue(this.producerConfig, Properties.class);
        try (final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig)) {
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, record);
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error sending record {} over kafka to topic {}.",
                            record, topic, exception);
                }
            });
        }
    }
}
