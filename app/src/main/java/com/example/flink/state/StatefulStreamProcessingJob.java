package com.example.flink.state;

import com.example.flink.data.Event;
import com.example.flink.data.EventDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Stateful stream processing demonstration showcasing:
 * - ValueState: Single value per key
 * - ListState: List of values per key
 * - MapState: Key-value map per key
 * - ReducingState: Incrementally reduced values
 * - AggregatingState: Aggregated values
 * - State TTL: Automatic state cleanup
 * - State backends and checkpointing
 */
public class StatefulStreamProcessingJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(StatefulStreamProcessingJob.class);
    
    public static void execute(StreamExecutionEnvironment env) throws Exception {
        LOG.info("Starting Stateful Stream Processing Job");
        
        // Configure Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("events")
                .setGroupId("stateful-consumer")
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
        
        // Demo 1: User session tracking with ValueState
        DataStream<UserSession> sessionStream = eventStream
                .keyBy(Event::userId)
                .flatMap(new UserSessionTracker())
                .name("user-session-tracking");
        
        // Demo 2: Event counting with ReducingState
        DataStream<UserEventCount> eventCountStream = eventStream
                .keyBy(Event::userId)
                .flatMap(new EventCounter())
                .name("event-counting");
        
        // Demo 3: Activity pattern detection with ListState
        DataStream<ActivityPattern> patternStream = eventStream
                .keyBy(Event::userId)
                .flatMap(new ActivityPatternDetector())
                .name("activity-pattern-detection");
        
        // Demo 4: Feature aggregation with MapState
        DataStream<UserFeatures> featureStream = eventStream
                .keyBy(Event::userId)
                .flatMap(new FeatureAggregator())
                .name("feature-aggregation");
        
        // Print results
        sessionStream.print("User Sessions: ");
        eventCountStream.print("Event Counts: ");
        patternStream.print("Activity Patterns: ");
        featureStream.print("User Features: ");
        
        LOG.info("Stateful Stream Processing Job configured successfully");
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
    
    /**
     * User session tracker using ValueState
     * Tracks user sessions with automatic timeout
     */
    private static class UserSessionTracker extends RichFlatMapFunction<Event, UserSession> {
        
        private transient ValueState<UserSessionState> sessionState;
        
        @Override
        public void open(Configuration parameters) {
            // Configure state with TTL
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.minutes(30))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            
            ValueStateDescriptor<UserSessionState> descriptor = new ValueStateDescriptor<>(
                    "user-session-state",
                    UserSessionState.class
            );
            descriptor.enableTimeToLive(ttlConfig);
            
            sessionState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void flatMap(Event event, Collector<UserSession> out) throws Exception {
            UserSessionState current = sessionState.value();
            
            if (current == null) {
                // Start new session
                current = new UserSessionState(
                        event.userId(),
                        event.timestamp(),
                        event.timestamp(),
                        1,
                        new HashSet<>(Arrays.asList(event.type()))
                );
            } else {
                // Update existing session
                current.lastActivity = event.timestamp();
                current.eventCount++;
                current.eventTypes.add(event.type());
                
                // Check for session timeout (15 minutes)
                if (event.timestamp() - current.lastActivity > 15 * 60 * 1000) {
                    // Emit completed session
                    out.collect(new UserSession(
                            current.userId,
                            current.sessionStart,
                            current.lastActivity,
                            current.eventCount,
                            current.eventTypes.size()
                    ));
                    
                    // Start new session
                    current = new UserSessionState(
                            event.userId(),
                            event.timestamp(),
                            event.timestamp(),
                            1,
                            new HashSet<>(Arrays.asList(event.type()))
                    );
                }
            }
            
            sessionState.update(current);
        }
    }
    
    /**
     * Event counter using ReducingState
     */
    private static class EventCounter extends RichFlatMapFunction<Event, UserEventCount> {
        
        private transient ReducingState<Long> eventCountState;
        private transient ValueState<Long> lastEmitTime;
        
        @Override
        public void open(Configuration parameters) {
            ReducingStateDescriptor<Long> countDescriptor = new ReducingStateDescriptor<>(
                    "event-count",
                    Long::sum,
                    Long.class
            );
            eventCountState = getRuntimeContext().getReducingState(countDescriptor);
            
            ValueStateDescriptor<Long> timeDescriptor = new ValueStateDescriptor<>(
                    "last-emit-time",
                    Long.class
            );
            lastEmitTime = getRuntimeContext().getState(timeDescriptor);
        }
        
        @Override
        public void flatMap(Event event, Collector<UserEventCount> out) throws Exception {
            eventCountState.add(1L);
            
            Long lastEmit = lastEmitTime.value();
            long currentTime = System.currentTimeMillis();
            
            // Emit count every 5 minutes
            if (lastEmit == null || currentTime - lastEmit > 5 * 60 * 1000) {
                Long count = eventCountState.get();
                if (count != null) {
                    out.collect(new UserEventCount(event.userId(), count, currentTime));
                    lastEmitTime.update(currentTime);
                }
            }
        }
    }
    
    /**
     * Activity pattern detector using ListState
     */
    private static class ActivityPatternDetector extends RichFlatMapFunction<Event, ActivityPattern> {
        
        private transient ListState<String> recentEventsState;
        
        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                    "recent-events",
                    String.class
            );
            recentEventsState = getRuntimeContext().getListState(descriptor);
        }
        
        @Override
        public void flatMap(Event event, Collector<ActivityPattern> out) throws Exception {
            // Add current event to list
            recentEventsState.add(event.type());
            
            // Get all recent events
            List<String> recentEvents = new ArrayList<>();
            for (String eventType : recentEventsState.get()) {
                recentEvents.add(eventType);
            }
            
            // Keep only last 10 events
            if (recentEvents.size() > 10) {
                recentEventsState.clear();
                for (int i = recentEvents.size() - 10; i < recentEvents.size(); i++) {
                    recentEventsState.add(recentEvents.get(i));
                }
                recentEvents = recentEvents.subList(recentEvents.size() - 10, recentEvents.size());
            }
            
            // Detect patterns
            if (recentEvents.size() >= 3) {
                String pattern = detectPattern(recentEvents);
                if (pattern != null) {
                    out.collect(new ActivityPattern(event.userId(), pattern, recentEvents.size()));
                }
            }
        }
        
        private String detectPattern(List<String> events) {
            // Check for purchase funnel pattern
            if (events.contains("view") && events.contains("add_to_cart") && events.contains("purchase")) {
                return "PURCHASE_FUNNEL";
            }
            
            // Check for browsing pattern
            long viewCount = events.stream().filter(e -> "view".equals(e)).count();
            if (viewCount >= 5) {
                return "HEAVY_BROWSING";
            }
            
            // Check for engagement pattern
            Set<String> uniqueEvents = new HashSet<>(events);
            if (uniqueEvents.size() >= 4) {
                return "HIGH_ENGAGEMENT";
            }
            
            return null;
        }
    }
    
    /**
     * Feature aggregator using MapState
     */
    private static class FeatureAggregator extends RichFlatMapFunction<Event, UserFeatures> {
        
        private transient MapState<String, Double> featureState;
        private transient ValueState<Long> lastEmitTime;
        
        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Double> descriptor = new MapStateDescriptor<>(
                    "user-features",
                    String.class,
                    Double.class
            );
            featureState = getRuntimeContext().getMapState(descriptor);
            
            ValueStateDescriptor<Long> timeDescriptor = new ValueStateDescriptor<>(
                    "last-feature-emit",
                    Long.class
            );
            lastEmitTime = getRuntimeContext().getState(timeDescriptor);
        }
        
        @Override
        public void flatMap(Event event, Collector<UserFeatures> out) throws Exception {
            // Update features based on event
            updateFeatures(event);
            
            Long lastEmit = lastEmitTime.value();
            long currentTime = System.currentTimeMillis();
            
            // Emit features every 10 minutes
            if (lastEmit == null || currentTime - lastEmit > 10 * 60 * 1000) {
                Map<String, Double> features = new HashMap<>();
                for (Map.Entry<String, Double> entry : featureState.entries()) {
                    features.put(entry.getKey(), entry.getValue());
                }
                
                if (!features.isEmpty()) {
                    out.collect(new UserFeatures(event.userId(), features, currentTime));
                    lastEmitTime.update(currentTime);
                }
            }
        }
        
        private void updateFeatures(Event event) throws Exception {
            // Event type frequency
            String eventTypeKey = "event_type_" + event.type();
            Double currentCount = featureState.get(eventTypeKey);
            featureState.put(eventTypeKey, (currentCount == null ? 0.0 : currentCount) + 1.0);
            
            // Time-based features
            String hourKey = "hour_" + (event.timestamp() / 1000 / 3600 % 24);
            Double hourCount = featureState.get(hourKey);
            featureState.put(hourKey, (hourCount == null ? 0.0 : hourCount) + 1.0);
            
            // Data-based features
            if (event.data().containsKey("amount")) {
                try {
                    double amount = Double.parseDouble(event.data().get("amount").toString());
                    Double totalAmount = featureState.get("total_amount");
                    featureState.put("total_amount", (totalAmount == null ? 0.0 : totalAmount) + amount);
                    
                    Double maxAmount = featureState.get("max_amount");
                    featureState.put("max_amount", Math.max(maxAmount == null ? 0.0 : maxAmount, amount));
                } catch (NumberFormatException e) {
                    // Ignore invalid amounts
                }
            }
        }
    }
    
    // State classes
    private static class UserSessionState {
        String userId;
        long sessionStart;
        long lastActivity;
        int eventCount;
        Set<String> eventTypes;
        
        public UserSessionState() {}
        
        public UserSessionState(String userId, long sessionStart, long lastActivity, 
                              int eventCount, Set<String> eventTypes) {
            this.userId = userId;
            this.sessionStart = sessionStart;
            this.lastActivity = lastActivity;
            this.eventCount = eventCount;
            this.eventTypes = eventTypes;
        }
    }
    
    // Result classes
    public record UserSession(String userId, long sessionStart, long sessionEnd, 
                            int eventCount, int uniqueEventTypes) {}
    
    public record UserEventCount(String userId, long totalEvents, long timestamp) {}
    
    public record ActivityPattern(String userId, String pattern, int eventCount) {}
    
    public record UserFeatures(String userId, Map<String, Double> features, long timestamp) {}
}
