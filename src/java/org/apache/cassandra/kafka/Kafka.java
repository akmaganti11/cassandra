package org.apache.cassandra.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Kafka {
	private static final Logger logger = LoggerFactory.getLogger(Kafka.class);
	private final Properties properties = new Properties();
	Long FLUSH_THRESHOLD = 100l;
	Long FLUSH_TIME_MS = 1000l;
	Integer FLUSH_RETRIES = 3;
	Boolean PUBLISH_MUTATION_EVENT=true;
	Boolean PUBLISH_AUTH_EVENT=true;
	KafkaProducer<String, String> PRODUCER = null;
	
	
	public Kafka() {
		try {
			loadKafkaConfig();
			initProcuder();
			initKafkaTopicsManager();
		} catch (IOException e) {
			logger.debug(">>>>> Could not load Kakfa Properties - {}", e.getMessage());
			logger.debug(">>>>> Stopping Cassandra due to Kafka config issue");
			System.exit(0);
		}
	}
	
	public void initKafkaTopicsManager() {
		new KafkaTopicManager();
	}
	
	public void loadKafkaConfig() throws IOException {
		String kafkaPropsPath = System.getProperty("kafka.properties");
		if (null == kafkaPropsPath) {
			logger.debug(">>>>> Kafka Properties not set. Pass kafka.properties file path in argumnets!");
			throw new IllegalArgumentException("Kafka Properties path not set!");
		}
		InputStream resourceAsStream = new FileInputStream(kafkaPropsPath);
		properties.load(resourceAsStream);
	}

	private void initProcuder() {
		PRODUCER = new KafkaProducer<String, String>(properties);
		
		//PRODUCER Flush records threshold
		if (null == properties.get(KafkaConfigs.PRODUCER_FLUSH_THRESHOLD).toString()) {
			logger.debug(">>>>> Producer Record Flush threshold not set, setting to default - {}", FLUSH_THRESHOLD);
		} else {
			FLUSH_THRESHOLD = Long.parseLong(properties.get("producer.flush.threshold").toString());
		}
		
		//Flush retry config
		if (null == properties.get(KafkaConfigs.FLUSH_RETRY_TIME_MS).toString()) {
			logger.debug(">>>>> Batch Flush retry not set, setting to default - {}", FLUSH_TIME_MS);
		} else {
			FLUSH_TIME_MS = Long.parseLong(properties.get(KafkaConfigs.FLUSH_RETRY_TIME_MS).toString());
		}
		
		//Flush retries config
		if (null == properties.get(KafkaConfigs.FLUSH_RETRIES).toString()) {
			logger.debug(">>>>> Batch Flush retry not set, setting to default - {}", FLUSH_RETRIES);
		} else {
			FLUSH_RETRIES = Integer.parseInt(properties.get(KafkaConfigs.FLUSH_RETRIES).toString());
		}
		
		//Publish Mutations Events Config
		if (null == properties.get(KafkaConfigs.PUBLISH_MUTATION_EVENTS).toString()) {
			logger.debug(">>>>> Publish Mutation Events not set, setting to default - {}", PUBLISH_MUTATION_EVENT);
		} else {
			PUBLISH_MUTATION_EVENT = Boolean.parseBoolean(properties.get(KafkaConfigs.PUBLISH_MUTATION_EVENTS).toString());
		}
		
		//Publish Auth Events Config
		if (null == properties.get(KafkaConfigs.PUBLISH_AUTH_EVENTS).toString()) {
			logger.debug(">>>>> Publish Auth Events not set, setting to default - {}", PUBLISH_AUTH_EVENT);
		} else {
			PUBLISH_AUTH_EVENT = Boolean.parseBoolean(properties.get(KafkaConfigs.PUBLISH_AUTH_EVENTS).toString());
		}		
		
	}
}

