package com.example.flink.streaming;

import com.example.flink.data.Event;
import com.example.flink.data.EventDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Real-time analytics job demonstrating advanced stream processing patterns:
 * - Event-time processing with watermarks
 * - Windowing aggregations
 * - Multiple analytics streams
 * - Performance metrics
 */
public class RealTimeAnalyticsJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(RealTimeAnalyticsJob.class);
    
    public static void execute(StreamExecutionEnvironment env) throws Exception {
        LOG.info("Starting Real-Time Analytics Job");
        
        // Configure Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("events")
                .setGroupId("analytics-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Create event stream with watermarks
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
        
        // Analytics Stream 1: Event count per type per minute
        SingleOutputStreamOperator<EventMetrics> eventCountMetrics = eventStream
                .keyBy(Event::type)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new EventCountAggregator())
                .name("event-count-metrics");
        
        // Analytics Stream 2: User activity metrics
        SingleOutputStreamOperator<UserActivityMetrics> userActivityMetrics = eventStream
                .keyBy(Event::userId)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new UserActivityAggregator())
                .name("user-activity-metrics");
        
        // Analytics Stream 3: Revenue metrics (for purchase events)
        SingleOutputStreamOperator<RevenueMetrics> revenueMetrics = eventStream
                .filter(event -> "purchase".equals(event.type()) && event.data().containsKey("amount"))
                .keyBy(event -> "global") // Global revenue aggregation
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new RevenueAggregator())
                .name("revenue-metrics");
        
        // Print analytics results
        eventCountMetrics.print("Event Count Metrics: ");
        userActivityMetrics.print("User Activity Metrics: ");
        revenueMetrics.print("Revenue Metrics: ");
        
        LOG.info("Real-Time Analytics Job configured successfully");
    }
    
    /**
     * Map function to deserialize JSON events
     */
    private static class EventDeserializationMapper implements MapFunction<String, Event> {
        @Override
        public Event map(String value) throws Exception {
            return EventDeserializer.deserialize(value.getBytes());
        }
    }
    
    /**
     * Aggregate function for counting events by type
     */
    private static class EventCountAggregator implements AggregateFunction<Event, EventCountAccumulator, EventMetrics> {
        
        @Override
        public EventCountAccumulator createAccumulator() {
            return new EventCountAccumulator();
        }
        
        @Override
        public EventCountAccumulator add(Event event, EventCountAccumulator accumulator) {
            accumulator.count++;
            accumulator.eventType = event.type();
            accumulator.windowEnd = System.currentTimeMillis();
            return accumulator;
        }
        
        @Override
        public EventMetrics getResult(EventCountAccumulator accumulator) {
            return new EventMetrics(accumulator.eventType, accumulator.count, accumulator.windowEnd);
        }
        
        @Override
        public EventCountAccumulator merge(EventCountAccumulator a, EventCountAccumulator b) {
            a.count += b.count;
            a.windowEnd = Math.max(a.windowEnd, b.windowEnd);
            return a;
        }
    }
    
    /**
     * Aggregate function for user activity metrics
     */
    private static class UserActivityAggregator implements AggregateFunction<Event, UserActivityAccumulator, UserActivityMetrics> {
        
        @Override
        public UserActivityAccumulator createAccumulator() {
            return new UserActivityAccumulator();
        }
        
        @Override
        public UserActivityAccumulator add(Event event, UserActivityAccumulator accumulator) {
            accumulator.userId = event.userId();
            accumulator.eventCount++;
            accumulator.lastActivity = event.timestamp();
            accumulator.uniqueEventTypes.add(event.type());
            return accumulator;
        }
        
        @Override
        public UserActivityMetrics getResult(UserActivityAccumulator accumulator) {
            return new UserActivityMetrics(
                    accumulator.userId,
                    accumulator.eventCount,
                    accumulator.uniqueEventTypes.size(),
                    accumulator.lastActivity
            );
        }
        
        @Override
        public UserActivityAccumulator merge(UserActivityAccumulator a, UserActivityAccumulator b) {
            a.eventCount += b.eventCount;
            a.lastActivity = Math.max(a.lastActivity, b.lastActivity);
            a.uniqueEventTypes.addAll(b.uniqueEventTypes);
            return a;
        }
    }
    
    /**
     * Aggregate function for revenue metrics
     */
    private static class RevenueAggregator implements AggregateFunction<Event, RevenueAccumulator, RevenueMetrics> {
        
        @Override
        public RevenueAccumulator createAccumulator() {
            return new RevenueAccumulator();
        }
        
        @Override
        public RevenueAccumulator add(Event event, RevenueAccumulator accumulator) {
            try {
                double amount = Double.parseDouble(event.data().get("amount").toString());
                accumulator.totalRevenue += amount;
                accumulator.transactionCount++;
                accumulator.maxTransaction = Math.max(accumulator.maxTransaction, amount);
                accumulator.minTransaction = Math.min(accumulator.minTransaction, amount);
            } catch (Exception e) {
                LOG.warn("Could not parse amount from event: {}", event);
            }
            return accumulator;
        }
        
        @Override
        public RevenueMetrics getResult(RevenueAccumulator accumulator) {
            double avgTransaction = accumulator.transactionCount > 0 
                    ? accumulator.totalRevenue / accumulator.transactionCount : 0.0;
            return new RevenueMetrics(
                    accumulator.totalRevenue,
                    accumulator.transactionCount,
                    avgTransaction,
                    accumulator.maxTransaction,
                    accumulator.minTransaction
            );
        }
        
        @Override
        public RevenueAccumulator merge(RevenueAccumulator a, RevenueAccumulator b) {
            a.totalRevenue += b.totalRevenue;
            a.transactionCount += b.transactionCount;
            a.maxTransaction = Math.max(a.maxTransaction, b.maxTransaction);
            a.minTransaction = Math.min(a.minTransaction, b.minTransaction);
            return a;
        }
    }
    
    // Accumulator classes
    private static class EventCountAccumulator {
        long count = 0;
        String eventType = "";
        long windowEnd = 0;
    }
    
    private static class UserActivityAccumulator {
        String userId = "";
        int eventCount = 0;
        long lastActivity = 0;
        java.util.Set<String> uniqueEventTypes = new java.util.HashSet<>();
    }
    
    private static class RevenueAccumulator {
        double totalRevenue = 0.0;
        int transactionCount = 0;
        double maxTransaction = Double.MIN_VALUE;
        double minTransaction = Double.MAX_VALUE;
    }
    
    // Result classes
    public record EventMetrics(String eventType, long count, long windowEnd) {}
    
    public record UserActivityMetrics(String userId, int eventCount, int uniqueEventTypes, long lastActivity) {}
    
    public record RevenueMetrics(double totalRevenue, int transactionCount, double avgTransaction, 
                               double maxTransaction, double minTransaction) {}
}
