package org.apache.cassandra.kafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.assertj.core.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicManager {
	private static final Logger logger = LoggerFactory.getLogger(KafkaTopicManager.class);

	private Set<String> allTopics = new HashSet<>();
	Properties properties = new Properties();
	String TOPICS = null;

	public KafkaTopicManager() {
			initKafkaProps();
			initTopics();
	}

	public AdminClient createAdminClient() throws IOException {
		return AdminClient.create(properties);
	}

	private void initKafkaProps() {
		try {
			String kafkaPropsPath = System.getProperty("kafka.properties");
			if (null == kafkaPropsPath) {
				logger.debug(">>>>> Kafka Properties not set. Pass kafka.properties file path in argumnets!");
				throw new IllegalArgumentException("Kafka Properties path not set!");
			}
			InputStream resourceAsStream = new FileInputStream(kafkaPropsPath);
			properties.load(resourceAsStream);
		} catch (IOException e) {
			logger.debug(">>>>> Kafka Properties not set. Issue in loading Kafka props");
		}
	}

	public void createTopics(final Set<String> topics) throws InterruptedException, ExecutionException, IOException {
		Admin adminClient = createAdminClient();
		
		String regex = "^[a-zA-Z0-9\\-\\.]+$";
		Pattern.compile(regex);

		List<String> validTopicsList = topics.stream().filter(t -> t.matches(regex)).collect(Collectors.toList());
		if (validTopicsList.isEmpty()) {
			throw new IllegalArgumentException("INVALID TOPIC NAMES!");
		}

		ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
		listTopicsOptions.listInternal(true);
		Set<String> topicNames = adminClient.listTopics(listTopicsOptions).names().get();

		validTopicsList.forEach(topic -> {
			if (!topicNames.contains(topic)) {
				NewTopic newTopic = new NewTopic(topic, 1, (short) 1); // new NewTopic(topicName, numPartitions,
				// replicationFactor)

				List<NewTopic> newTopics = new ArrayList<NewTopic>();
				newTopics.add(newTopic);

				adminClient.createTopics(newTopics);
				logger.debug(">>>>> New Topics Created: {}", topic);
				allTopics.add(topic);

			} else {
				logger.debug(">>>>> Topics Exists!!");
				allTopics.addAll(topicNames);
			}
		});
		
		adminClient.close();
	}

	public DeleteTopicsResult deleteTopics(final Set<String> topics) throws InterruptedException, ExecutionException, IOException {
		Admin adminClient = createAdminClient();
		Set<String> topicNames = adminClient.listTopics().names().get();
		Set<String> topicsToDelete = topics.stream().filter(topic -> topicNames.contains(topic))
				.collect(Collectors.toSet());
		DeleteTopicsResult deletedTopicsResult = adminClient.deleteTopics(topicsToDelete);
		topics.removeAll(topicsToDelete);
		if (!topics.isEmpty()) {
			logger.debug("Topics doesnot exists to Delete: {}" + topics);
		}

		adminClient.close();
		return deletedTopicsResult;
	}

	public Set<String> getTopics() {
		return allTopics;
	}

	public void addTopic(String topic) {
		this.allTopics.add(topic);
	}

	public void addTopics(Set<String> topics) {
		this.allTopics.addAll(topics);
	}

	public void initTopics() {
		// Topics Config
		if (null == properties.get(KafkaConfigs.TOPICS).toString()) {
			logger.debug(">>>>> Topics is not set, setting to default - {}", TOPICS);
		} else {
			TOPICS = properties.get(KafkaConfigs.TOPICS).toString();
			String[] topics = TOPICS.split(",");
			List<Object> topicsList = Arrays.asList(topics);
			
			try {
				createTopics(topicsList.stream().map(t -> t.toString()).collect(Collectors.toSet()));
				
				ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
				listTopicsOptions.listInternal(true);
				
				try {
					Admin adminClient = createAdminClient();
					Set<String> topicNames = adminClient.listTopics(listTopicsOptions).names().get();
					topicNames.forEach(t -> {
						logger.debug(">>>>> Topics: {}", t);
					});
					
					adminClient.close();
				} catch (Exception e) {
					logger.debug(">>>>> Could read topics from Admin Client - {}", e.getMessage());
				}
			} catch (InterruptedException | ExecutionException | IOException e) {
				logger.debug(">>>>> Exception in iterating Topics provided in config - {}", topicsList.toArray());
			}

		}
	}
}
