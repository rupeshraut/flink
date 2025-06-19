package com.example.flink.sql;

import com.example.flink.data.Event;
import com.example.flink.data.EventDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink SQL demonstration showcasing:
 * - Stream to Table conversion
 * - Complex SQL queries
 * - Window functions in SQL
 * - Temporal joins
 * - Watermarks and event time in SQL
 * - User-defined functions
 * - Continuous queries
 */
public class FlinkSQLDemo {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLDemo.class);
    
    public static void execute(StreamExecutionEnvironment env) throws Exception {
        LOG.info("Starting Flink SQL Demo");
        
        // Create table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Configure Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("events")
                .setGroupId("sql-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Create event stream
        DataStream<Event> eventStream = env
                .fromSource(kafkaSource, WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            try {
                                return EventDeserializer.deserialize(event.getBytes()).timestamp();
                            } catch (Exception e) {
                                return timestamp;
                            }
                        }), "kafka-source")
                .map(new EventDeserializationMapper())
                .name("deserialize-events");
        
        // Convert stream to table with event-time attributes
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("userId"),
                $("type"),
                $("timestamp").rowtime(),
                $("data")
        );
        
        // Register the table for SQL queries
        tableEnv.createTemporaryView("events", eventTable);
        
        // Demo 1: Basic aggregation queries
        demonstrateBasicQueries(tableEnv);
        
        // Demo 2: Window functions
        demonstrateWindowFunctions(tableEnv);
        
        // Demo 3: Complex analytical queries
        demonstrateAnalyticalQueries(tableEnv);
        
        // Demo 4: Temporal patterns
        demonstrateTemporalPatterns(tableEnv);
        
        LOG.info("Flink SQL Demo configured successfully");
    }
    
    /**
     * Basic SQL aggregation queries
     */
    private static void demonstrateBasicQueries(StreamTableEnvironment tableEnv) {
        LOG.info("Running basic SQL queries");
        
        // Query 1: Event count by type
        Table eventCounts = tableEnv.sqlQuery(
            "SELECT type, COUNT(*) as event_count " +
            "FROM events " +
            "GROUP BY type"
        );
        
        // Query 2: User activity summary
        Table userActivity = tableEnv.sqlQuery(
            "SELECT userId, " +
            "       COUNT(*) as total_events, " +
            "       COUNT(DISTINCT type) as unique_event_types, " +
            "       MAX(timestamp) as last_activity " +
            "FROM events " +
            "GROUP BY userId"
        );
        
        // Convert back to streams and print
        tableEnv.toChangelogStream(eventCounts).print("Event Counts: ");
        tableEnv.toChangelogStream(userActivity).print("User Activity: ");
    }
    
    /**
     * Window functions demonstration
     */
    private static void demonstrateWindowFunctions(StreamTableEnvironment tableEnv) {
        LOG.info("Running window function queries");
        
        // Tumbling window aggregation
        Table tumblingWindow = tableEnv.sqlQuery(
            "SELECT " +
            "  type, " +
            "  TUMBLE_START(timestamp, INTERVAL '5' MINUTE) as window_start, " +
            "  TUMBLE_END(timestamp, INTERVAL '5' MINUTE) as window_end, " +
            "  COUNT(*) as event_count, " +
            "  COUNT(DISTINCT userId) as unique_users " +
            "FROM events " +
            "GROUP BY type, TUMBLE(timestamp, INTERVAL '5' MINUTE)"
        );
        
        // Sliding window for moving averages
        Table slidingWindow = tableEnv.sqlQuery(
            "SELECT " +
            "  userId, " +
            "  HOP_START(timestamp, INTERVAL '2' MINUTE, INTERVAL '10' MINUTE) as window_start, " +
            "  HOP_END(timestamp, INTERVAL '2' MINUTE, INTERVAL '10' MINUTE) as window_end, " +
            "  COUNT(*) as event_count, " +
            "  COUNT(*) / 10.0 as events_per_minute " +
            "FROM events " +
            "GROUP BY userId, HOP(timestamp, INTERVAL '2' MINUTE, INTERVAL '10' MINUTE)"
        );
        
        tableEnv.toChangelogStream(tumblingWindow).print("Tumbling Window: ");
        tableEnv.toChangelogStream(slidingWindow).print("Sliding Window: ");
    }
    
    /**
     * Complex analytical queries
     */
    private static void demonstrateAnalyticalQueries(StreamTableEnvironment tableEnv) {
        LOG.info("Running complex analytical queries");
        
        // User engagement scoring
        Table engagementScore = tableEnv.sqlQuery(
            "SELECT " +
            "  userId, " +
            "  COUNT(*) as total_events, " +
            "  COUNT(DISTINCT type) as event_variety, " +
            "  COUNT(DISTINCT type) * COUNT(*) as engagement_score, " +
            "  CASE " +
            "    WHEN COUNT(DISTINCT type) * COUNT(*) > 100 THEN 'HIGH' " +
            "    WHEN COUNT(DISTINCT type) * COUNT(*) > 50 THEN 'MEDIUM' " +
            "    ELSE 'LOW' " +
            "  END as engagement_level " +
            "FROM events " +
            "GROUP BY userId"
        );
        
        // Event sequence analysis
        Table eventSequences = tableEnv.sqlQuery(
            "SELECT " +
            "  userId, " +
            "  type as current_event, " +
            "  LAG(type, 1) OVER (PARTITION BY userId ORDER BY timestamp) as previous_event, " +
            "  LEAD(type, 1) OVER (PARTITION BY userId ORDER BY timestamp) as next_event, " +
            "  timestamp " +
            "FROM events"
        );
        
        // Funnel analysis
        Table funnelAnalysis = tableEnv.sqlQuery(
            "WITH user_events AS ( " +
            "  SELECT userId, " +
            "         MAX(CASE WHEN type = 'view' THEN 1 ELSE 0 END) as has_view, " +
            "         MAX(CASE WHEN type = 'add_to_cart' THEN 1 ELSE 0 END) as has_add_to_cart, " +
            "         MAX(CASE WHEN type = 'purchase' THEN 1 ELSE 0 END) as has_purchase " +
            "  FROM events " +
            "  GROUP BY userId " +
            ") " +
            "SELECT " +
            "  'Step 1: View' as funnel_step, " +
            "  SUM(has_view) as users, " +
            "  SUM(has_view) * 100.0 / COUNT(*) as conversion_rate " +
            "FROM user_events " +
            "UNION ALL " +
            "SELECT " +
            "  'Step 2: Add to Cart' as funnel_step, " +
            "  SUM(has_add_to_cart) as users, " +
            "  SUM(has_add_to_cart) * 100.0 / SUM(has_view) as conversion_rate " +
            "FROM user_events " +
            "WHERE has_view = 1 " +
            "UNION ALL " +
            "SELECT " +
            "  'Step 3: Purchase' as funnel_step, " +
            "  SUM(has_purchase) as users, " +
            "  SUM(has_purchase) * 100.0 / SUM(has_add_to_cart) as conversion_rate " +
            "FROM user_events " +
            "WHERE has_add_to_cart = 1"
        );
        
        tableEnv.toChangelogStream(engagementScore).print("Engagement Score: ");
        tableEnv.toChangelogStream(eventSequences).print("Event Sequences: ");
        tableEnv.toChangelogStream(funnelAnalysis).print("Funnel Analysis: ");
    }
    
    /**
     * Temporal pattern queries
     */
    private static void demonstrateTemporalPatterns(StreamTableEnvironment tableEnv) {
        LOG.info("Running temporal pattern queries");
        
        // Time-based patterns
        Table timePatterns = tableEnv.sqlQuery(
            "SELECT " +
            "  userId, " +
            "  EXTRACT(HOUR FROM timestamp) as hour_of_day, " +
            "  COUNT(*) as event_count, " +
            "  AVG(COUNT(*)) OVER (PARTITION BY userId) as avg_hourly_events " +
            "FROM events " +
            "GROUP BY userId, EXTRACT(HOUR FROM timestamp)"
        );
        
        // Session detection using SQL
        Table sessions = tableEnv.sqlQuery(
            "WITH event_gaps AS ( " +
            "  SELECT userId, type, timestamp, " +
            "         LAG(timestamp, 1, timestamp) OVER (PARTITION BY userId ORDER BY timestamp) as prev_timestamp " +
            "  FROM events " +
            "), " +
            "session_markers AS ( " +
            "  SELECT userId, type, timestamp, prev_timestamp, " +
            "         CASE WHEN timestamp - prev_timestamp > INTERVAL '15' MINUTE THEN 1 ELSE 0 END as new_session " +
            "  FROM event_gaps " +
            "), " +
            "session_ids AS ( " +
            "  SELECT userId, type, timestamp, " +
            "         SUM(new_session) OVER (PARTITION BY userId ORDER BY timestamp) as session_id " +
            "  FROM session_markers " +
            ") " +
            "SELECT " +
            "  userId, " +
            "  session_id, " +
            "  MIN(timestamp) as session_start, " +
            "  MAX(timestamp) as session_end, " +
            "  COUNT(*) as events_in_session, " +
            "  COUNT(DISTINCT type) as unique_event_types " +
            "FROM session_ids " +
            "GROUP BY userId, session_id"
        );
        
        tableEnv.toChangelogStream(timePatterns).print("Time Patterns: ");
        tableEnv.toChangelogStream(sessions).print("Sessions: ");
    }
    
    /**
     * Event deserialization mapper
     */
    private static class EventDeserializationMapper implements MapFunction<String, Event> {
        @Override
        public Event map(String value) throws Exception {
            return EventDeserializer.deserialize(value.getBytes());
        }
    }
}
