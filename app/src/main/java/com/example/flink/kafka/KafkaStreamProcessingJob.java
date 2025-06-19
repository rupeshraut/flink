package com.example.flink.kafka;

import com.example.flink.data.Event;
import com.example.flink.data.EventDeserializer;
import com.example.flink.data.EventSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Advanced Kafka Stream Processing with Flink
 * 
 * Demonstrates:
 * - Kafka source and sink connectors
 * - Event-time processing with watermarks
 * - Stream transformations and aggregations
 * - Windowing over Kafka streams
 * - Error handling and monitoring
 */
public class KafkaStreamProcessingJob {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamProcessingJob.class);
    
    // Kafka configuration
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String INPUT_TOPIC = "events-input";
    private static final String OUTPUT_TOPIC = "events-processed";
    private static final String CONSUMER_GROUP = "flink-advanced-consumer";
    
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        logger.info("🌊 Starting Kafka Stream Processing Demo");
        
        // Create Kafka source for reading events
        KafkaSource<Event> source = createKafkaSource();
        
        // Create Kafka sink for writing results
        KafkaSink<String> sink = createKafkaSink();
        
        // Build the streaming pipeline
        DataStream<Event> events = env
            .fromSource(source, WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().toEpochMilli()), "kafka-events")
            .name("kafka-event-source");
        
        // Transform and process events
        DataStream<String> processedEvents = events
            .filter(event -> event.getValue() > 0) // Filter valid events
            .keyBy(Event::getUserId) // Key by user ID for parallel processing
            .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 1-minute tumbling windows
            .process(new EventAggregationFunction()) // Custom aggregation
            .name("event-aggregation");
        
        // Enrich events with additional information
        DataStream<String> enrichedEvents = processedEvents
            .map(new EventEnrichmentFunction())
            .name("event-enrichment");
        
        // Sink processed events back to Kafka
        enrichedEvents.sinkTo(sink).name("kafka-output-sink");
        
        // Print to console for demo purposes
        enrichedEvents.print().name("console-output");
        
        logger.info("✅ Kafka stream processing pipeline configured");
        logger.info("📥 Reading from topic: {}", INPUT_TOPIC);
        logger.info("📤 Writing to topic: {}", OUTPUT_TOPIC);
        logger.info("🔧 Using consumer group: {}", CONSUMER_GROUP);
        
        // Execute the job
        env.execute("Kafka Stream Processing Job");
    }
    
    /**
     * Create Kafka source with optimized configuration
     */
    private static KafkaSource<Event> createKafkaSource() {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS);
        kafkaProps.setProperty("group.id", CONSUMER_GROUP);
        kafkaProps.setProperty("auto.offset.reset", "latest");
        kafkaProps.setProperty("enable.auto.commit", "false"); // Flink manages offsets
        kafkaProps.setProperty("max.poll.records", "500");
        kafkaProps.setProperty("fetch.min.bytes", "1024");
        kafkaProps.setProperty("fetch.max.wait.ms", "500");
        
        return KafkaSource.<Event>builder()
            .setBootstrapServers(KAFKA_BROKERS)
            .setTopics(INPUT_TOPIC)
            .setGroupId(CONSUMER_GROUP)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new EventDeserializer())
            .setProperties(kafkaProps)
            .build();
    }
    
    /**
     * Create Kafka sink with optimized configuration
     */
    private static KafkaSink<String> createKafkaSink() {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS);
        kafkaProps.setProperty("acks", "all"); // Wait for all replicas
        kafkaProps.setProperty("retries", "3");
        kafkaProps.setProperty("batch.size", "16384");
        kafkaProps.setProperty("linger.ms", "5");
        kafkaProps.setProperty("compression.type", "snappy");
        
        return KafkaSink.<String>builder()
            .setBootstrapServers(KAFKA_BROKERS)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(OUTPUT_TOPIC)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .setKafkaProducerConfig(kafkaProps)
            .build();
    }
    
    /**
     * Custom window function for event aggregation
     */
    public static class EventAggregationFunction 
            extends ProcessWindowFunction<Event, String, String, TimeWindow> {
        
        @Override
        public void process(String userId, 
                          ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, 
                          Iterable<Event> events, 
                          Collector<String> out) throws Exception {
            
            long count = 0;
            double sum = 0.0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            
            for (Event event : events) {
                count++;
                sum += event.getValue();
                max = Math.max(max, event.getValue());
                min = Math.min(min, event.getValue());
            }
            
            double avg = count > 0 ? sum / count : 0.0;
            
            String result = String.format(
                "{\"userId\":\"%s\",\"windowStart\":%d,\"windowEnd\":%d," +
                "\"count\":%d,\"sum\":%.2f,\"avg\":%.2f,\"min\":%.2f,\"max\":%.2f}",
                userId, context.window().getStart(), context.window().getEnd(),
                count, sum, avg, min, max
            );
            
            out.collect(result);
            
            logger.debug("Processed window for user {}: {} events, avg value: {:.2f}", 
                         userId, count, avg);
        }
    }
    
    /**
     * Enrich events with additional metadata
     */
    public static class EventEnrichmentFunction implements MapFunction<String, String> {
        
        @Override
        public String map(String value) throws Exception {
            // Parse the aggregated event
            // Add enrichment data (could be from external systems, caches, etc.)
            String enriched = value.replaceFirst("\\{", 
                "{\"processedAt\":" + System.currentTimeMillis() + 
                ",\"processor\":\"flink-kafka-processor\",");
            
            return enriched;
        }
    }
}
