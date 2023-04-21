package org.apache.cassandra.kafka;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.auth.AuthEvents;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.assertj.core.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEventPublishManager implements QueryEvents.Listener, AuthEvents.Listener, Closeable {

	private static final Logger logger = LoggerFactory.getLogger(AuditLogManager.class);
	public static final KafkaEventPublishManager instance = new KafkaEventPublishManager();
	private KafkaEventProducer kafkaEventProducer;
	private Boolean CASSANDRA_KAFKA_STREAM = false;
	private final Properties properties = new Properties();

	private KafkaEventPublishManager() {
		if(CASSANDRA_KAFKA_STREAM) {
			kafkaEventProducer = new KafkaEventProducer();
			registerAsListener();
		} else {
			logger.debug(">>>>> Cassandra - Kafka Stream if not enabled");
		}
		
	}

	//Register for Event stream only if its enabled in prop level
	public void registerAsListener() {
		if(CASSANDRA_KAFKA_STREAM) {
			logger.debug(">>>>> CASSANDRA - KAFKA STREAMING ENABLED >>>>>");
			QueryEvents.instance.registerListener(this);
			AuthEvents.instance.registerListener(this);
		} else {
			logger.debug(">>>>> Cassandra - Kafka Stream if not enabled");
		}
	}

	public void loadKafkaConfig() throws IOException {
		String kafkaPropsPath = System.getProperty("kafka.properties");
		if (null == kafkaPropsPath) {
			logger.debug(">>>>> Kafka Properties not set. Pass kafka.properties file path in argumnets!");
			throw new IllegalArgumentException("Kafka Properties path not set!");
		}
		InputStream resourceAsStream = new FileInputStream(kafkaPropsPath);
		properties.load(resourceAsStream);

		// Get Ca ssandra Kafka Streaming Events Config
		if (null == properties.get(KafkaConfigs.CASSANDRA_KAFKA_STREAM).toString()) {
			logger.debug(">>>>> Publish Auth Events not set, setting to default - {}", CASSANDRA_KAFKA_STREAM);
		} else {
			CASSANDRA_KAFKA_STREAM = Boolean.parseBoolean(properties.get(KafkaConfigs.CASSANDRA_KAFKA_STREAM).toString());
		}
	}

	private void unregisterAsListener() {
		QueryEvents.instance.unregisterListener(this);
		AuthEvents.instance.unregisterListener(this);
	}

	public void querySuccess(CQLStatement statement, String query, QueryOptions options, QueryState state,
			long queryTime, Message.Response response) {
		AuditLogEntry entry = new AuditLogEntry.Builder(state).setType(statement.getAuditLogContext().auditLogEntryType)
				.setOperation(query).setTimestamp(queryTime).setScope(statement).setKeyspace(state, statement)
				.setOptions(options).build();
		PublishableMutationEvent(entry);
	}

	// Batch Not Supported
	private void PublishableMutationEvent(AuditLogEntry entry) {
		if (kafkaEventProducer.PUBLISH_MUTATION_EVENT) {
			if (entry.getType().equals(AuditLogEntryType.UPDATE) || entry.getType().equals(AuditLogEntryType.DELETE)) {
				try {
					String keyspace = entry.getKeyspace();
					String operation = entry.getOperation();
					String[] operationSplitArray = operation.split(" ");
					if (operationSplitArray.length > 0) {
						List<Object> opsList = Arrays.asList(operationSplitArray);
						Set<String> tableWithKeyspace = opsList.stream().filter(o -> o.toString().startsWith(keyspace))
								.map(o -> o.toString()).collect(Collectors.toSet());

						String topic = tableWithKeyspace.iterator().next();

						kafkaEventProducer.sendEvent(topic, keyspace + "_" + UUID.randomUUID().toString(), operation);
					}
				} catch (InterruptedException e) {
					logger.debug(">>>>> Exception in Sending Event to Kafka : {}", e.getMessage());
				}
			}
		}
	}

	private void PublishableAuthEvent(AuditLogEntry entry) {
		if (kafkaEventProducer.PUBLISH_AUTH_EVENT) {
			try {
				kafkaEventProducer.sendEvent("cassandra_events", "KEYYY", "VALLLLL");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void PublishableEventAny(AuditLogEntry entry) {
		try {
			kafkaEventProducer.sendEvent("cassandra_events", "KEYYY", "VALLLLL");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void executeSuccess(CQLStatement statement, String query, QueryOptions options, QueryState state,
			long queryTime, Message.Response response) {
		AuditLogEntry entry = new AuditLogEntry.Builder(state).setType(statement.getAuditLogContext().auditLogEntryType)
				.setOperation(query).setTimestamp(queryTime).setScope(statement).setKeyspace(state, statement)
				.setOptions(options).build();
		PublishableMutationEvent(entry);
		logger.debug(">>>>> Published Execute Success Event : {}", entry.getOperation());
	}

	public void batchSuccess(BatchStatement.Type batchType, List<? extends CQLStatement> statements,
			List<String> queries, List<List<ByteBuffer>> values, QueryOptions options, QueryState state, long queryTime,
			Message.Response response) {
		List<AuditLogEntry> entries = buildEntriesForBatch(statements, queries, state, options, queryTime);
		for (AuditLogEntry entry : entries) {
			PublishableMutationEvent(entry);
			logger.debug(">>>>> Published Batch Success Event : {}", entry.getOperation());
		}
	}

	private static List<AuditLogEntry> buildEntriesForBatch(List<? extends CQLStatement> statements,
			List<String> queries, QueryState state, QueryOptions options, long queryStartTimeMillis) {
		List<AuditLogEntry> auditLogEntries = new ArrayList<>(statements.size() + 1);
		UUID batchId = UUID.randomUUID();
		String queryString = String.format("BatchId:[%s] - BATCH of [%d] statements", batchId, statements.size());
		AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation(queryString).setOptions(options)
				.setTimestamp(queryStartTimeMillis).setBatch(batchId).setType(AuditLogEntryType.BATCH).build();
		auditLogEntries.add(entry);

		for (int i = 0; i < statements.size(); i++) {
			CQLStatement statement = statements.get(i);
			entry = new AuditLogEntry.Builder(state).setType(statement.getAuditLogContext().auditLogEntryType)
					.setOperation(queries.get(i)).setTimestamp(queryStartTimeMillis).setScope(statement)
					.setKeyspace(state, statement).setOptions(options).setBatch(batchId).build();
			auditLogEntries.add(entry);
		}

		return auditLogEntries;
	}

	public void prepareSuccess(CQLStatement statement, String query, QueryState state, long queryTime,
			ResultMessage.Prepared response) {
		AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation(query)
				.setType(AuditLogEntryType.PREPARE_STATEMENT).setScope(statement).setKeyspace(statement).build();
		PublishableMutationEvent(entry);
	}

	public void authSuccess(QueryState state) {
		AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation("LOGIN SUCCESSFUL")
				.setType(AuditLogEntryType.LOGIN_SUCCESS).build();
		PublishableAuthEvent(entry);
	}

	public void authFailure(QueryState state, Exception cause) {
		AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation("LOGIN FAILURE")
				.setType(AuditLogEntryType.LOGIN_ERROR).build();
		PublishableAuthEvent(entry);
	}

	@Override
	public void close() throws IOException {
		unregisterAsListener();
	}

}
