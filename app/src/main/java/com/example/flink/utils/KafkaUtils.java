package com.example.flink.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka utilities for setting up topics and producing test data.
 * Provides methods for:
 * - Topic creation and management
 * - Test data production
 * - Connection validation
 * - Configuration helpers
 */
public class KafkaUtils {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_TOPIC = "events";
    public static final int DEFAULT_PARTITIONS = 3;
    public static final short DEFAULT_REPLICATION_FACTOR = 1;
    
    /**
     * Create a Kafka topic if it doesn't exist
     */
    public static boolean createTopicIfNotExists(String bootstrapServers, String topicName, 
                                               int partitions, short replicationFactor) {
        Properties adminProps = new Properties();
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Check if topic exists
            if (adminClient.listTopics().names().get().contains(topicName)) {
                LOG.info("Topic '{}' already exists", topicName);
                return true;
            }
            
            // Create topic
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            
            result.all().get(); // Wait for completion
            LOG.info("Successfully created topic '{}' with {} partitions and replication factor {}", 
                    topicName, partitions, replicationFactor);
            return true;
            
        } catch (Exception e) {
            LOG.error("Failed to create topic '{}': {}", topicName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Create default event topic
     */
    public static boolean createDefaultEventTopic(String bootstrapServers) {
        return createTopicIfNotExists(bootstrapServers, DEFAULT_TOPIC, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR);
    }
    
    /**
     * Create a Kafka producer with default configuration
     */
    public static KafkaProducer<String, String> createStringProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Performance and reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        // Idempotence for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return new KafkaProducer<>(props);
    }
    
    /**
     * Send a message to Kafka topic
     */
    public static Future<RecordMetadata> sendMessage(KafkaProducer<String, String> producer, 
                                                   String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOG.error("Failed to send message to topic '{}': {}", topic, exception.getMessage());
            } else {
                LOG.debug("Message sent to topic '{}', partition {}, offset {}", 
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }
    
    /**
     * Send a message without key
     */
    public static Future<RecordMetadata> sendMessage(KafkaProducer<String, String> producer, 
                                                   String topic, String value) {
        return sendMessage(producer, topic, null, value);
    }
    
    /**
     * Test Kafka connectivity
     */
    public static boolean testConnection(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 5000);
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Try to get metadata (this will test the connection)
            producer.partitionsFor("__consumer_offsets"); // Internal topic that should always exist
            LOG.info("Successfully connected to Kafka at {}", bootstrapServers);
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to connect to Kafka at {}: {}", bootstrapServers, e.getMessage());
            return false;
        }
    }
    
    /**
     * Get producer configuration for high throughput
     */
    public static Properties getHighThroughputProducerConfig(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // High throughput settings
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // Leader acknowledgment only
        
        return props;
    }
    
    /**
     * Get producer configuration for low latency
     */
    public static Properties getLowLatencyProducerConfig(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Low latency settings
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024); // Small batches
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0); // Send immediately
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        return props;
    }
    
    /**
     * Get producer configuration for reliability
     */
    public static Properties getReliableProducerConfig(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300000); // 5 minutes
        
        return props;
    }
}
