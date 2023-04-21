/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventProducer implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaEventProducer.class);

	KafkaProducer<String, String> PRODUCER = null;
	Long KAFKA_FLUSH_THRESHOLD = null;
	Integer FLUSH_RETRIES = 3;
	Boolean PUBLISH_MUTATION_EVENT = true;
	Boolean PUBLISH_AUTH_EVENT = true;
	private List<ProducerRecord<String, String>> PRODUCER_RECORDS = new ArrayList<>();

	public KafkaEventProducer() {
		Kafka KAFKA = new Kafka();
		PRODUCER = KAFKA.PRODUCER;
		KAFKA_FLUSH_THRESHOLD = KAFKA.FLUSH_THRESHOLD;
		FLUSH_RETRIES = KAFKA.FLUSH_RETRIES;
		PUBLISH_MUTATION_EVENT = KAFKA.PUBLISH_MUTATION_EVENT;
		PUBLISH_AUTH_EVENT = KAFKA.PUBLISH_AUTH_EVENT;
		produceEvents();
	}

	public void produceEvents() {
		logger.debug(">>>>> Producing Kafka Events");
		if (FLUSH_RETRIES == 0 || PRODUCER_RECORDS.size() == KAFKA_FLUSH_THRESHOLD) {
			producerRecords(PRODUCER_RECORDS);
			if (PRODUCER_RECORDS.size() != 0) {
				logger.debug(">>>>> Produced {} Events", PRODUCER_RECORDS.size());
				PRODUCER.flush();
			}
		} else {
			--FLUSH_RETRIES;
			producerRecords(PRODUCER_RECORDS);
			if (PRODUCER_RECORDS.size() != 0) {
				logger.debug(">>>>> Produced {} Events", PRODUCER_RECORDS.size());
				PRODUCER.flush();
			}
		}
	}

	public void sendAsBatchEvents(Map<String, KafkaEvent> events) throws InterruptedException {
		events.forEach((topic, event) -> {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, event.getKey(),
					event.getValue());
			PRODUCER_RECORDS.add(producerRecord);
		});

		produceEvents();
	}

	public void sendEvent(String topic, String key, String value) throws InterruptedException {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
		PRODUCER.send(producerRecord);
		PRODUCER.flush();
		logger.debug(">>>>> Produced Event");
	}

	private void producerRecords(List<ProducerRecord<String, String>> PRODUCER_RECORDS) {
		PRODUCER_RECORDS.stream().forEach(producer_record -> {
			PRODUCER.send(producer_record);
		});
	}

	@Override
	public void close() throws IOException {
		PRODUCER.close();
	}

}
