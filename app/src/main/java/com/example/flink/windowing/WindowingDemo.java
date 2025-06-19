package com.example.flink.windowing;

import com.example.flink.data.Event;
import com.example.flink.data.EventDeserializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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
import org.apache.flink.streaming.api.windowing.assigners.SessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Comprehensive windowing demonstration showcasing:
 * - Tumbling windows
 * - Sliding windows
 * - Session windows
 * - Custom triggers
 * - Late data handling
 * - Window functions and aggregations
 */
public class WindowingDemo {
    
    private static final Logger LOG = LoggerFactory.getLogger(WindowingDemo.class);
    
    public static void execute(StreamExecutionEnvironment env) throws Exception {
        LOG.info("Starting Windowing Demo");
        
        // Configure Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("events")
                .setGroupId("windowing-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Create event stream with watermarks and late data handling
        DataStream<Event> eventStream = env
                .fromSource(kafkaSource, WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new EventTimestampAssigner()), "kafka-source")
                .map(new EventDeserializationMapper())
                .name("deserialize-events");
        
        // Demo 1: Tumbling Windows - Non-overlapping fixed-size windows
        demonstrateTumblingWindows(eventStream);
        
        // Demo 2: Sliding Windows - Overlapping fixed-size windows
        demonstrateSlidingWindows(eventStream);
        
        // Demo 3: Session Windows - Dynamic windows based on activity gaps
        demonstrateSessionWindows(eventStream);
        
        // Demo 4: Custom Triggers - Advanced window behavior
        demonstrateCustomTriggers(eventStream);
        
        LOG.info("Windowing Demo configured successfully");
    }
    
    /**
     * Tumbling windows: non-overlapping, fixed-size windows
     * Use case: Hourly aggregations, daily reports
     */
    private static void demonstrateTumblingWindows(DataStream<Event> eventStream) {
        LOG.info("Setting up Tumbling Windows demo");
        
        SingleOutputStreamOperator<WindowResult> tumblingResults = eventStream
                .keyBy(Event::type)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .allowedLateness(Time.minutes(1)) // Handle late data
                .aggregate(new EventCountAggregator(), new WindowMetadataFunction())
                .name("tumbling-window-aggregation");
        
        tumblingResults.print("Tumbling Window (5min): ");
    }
    
    /**
     * Sliding windows: overlapping, fixed-size windows
     * Use case: Moving averages, trend analysis
     */
    private static void demonstrateSlidingWindows(DataStream<Event> eventStream) {
        LOG.info("Setting up Sliding Windows demo");
        
        SingleOutputStreamOperator<WindowResult> slidingResults = eventStream
                .keyBy(Event::userId)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(2)))
                .allowedLateness(Time.minutes(1))
                .aggregate(new UserActivityAggregator(), new WindowMetadataFunction())
                .name("sliding-window-aggregation");
        
        slidingResults.print("Sliding Window (10min/2min): ");
    }
    
    /**
     * Session windows: dynamic windows based on inactivity gaps
     * Use case: User sessions, conversation analysis
     */
    private static void demonstrateSessionWindows(DataStream<Event> eventStream) {
        LOG.info("Setting up Session Windows demo");
        
        SingleOutputStreamOperator<WindowResult> sessionResults = eventStream
                .keyBy(Event::userId)
                .window(SessionWindows.withGap(Time.minutes(15))) // 15-minute inactivity gap
                .allowedLateness(Time.minutes(2))
                .aggregate(new SessionAggregator(), new WindowMetadataFunction())
                .name("session-window-aggregation");
        
        sessionResults.print("Session Window (15min gap): ");
    }
    
    /**
     * Custom triggers for advanced window behavior
     * Use case: Early firing, custom conditions
     */
    private static void demonstrateCustomTriggers(DataStream<Event> eventStream) {
        LOG.info("Setting up Custom Triggers demo");
        
        SingleOutputStreamOperator<WindowResult> customTriggerResults = eventStream
                .keyBy(Event::type)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(2))) // Fire every 2 minutes
                .allowedLateness(Time.minutes(1))
                .aggregate(new EventCountAggregator(), new WindowMetadataFunction())
                .name("custom-trigger-aggregation");
        
        customTriggerResults.print("Custom Trigger (2min fire): ");
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
     * Timestamp assigner for event-time processing
     */
    private static class EventTimestampAssigner implements SerializableTimestampAssigner<String> {
        @Override
        public long extractTimestamp(String element, long recordTimestamp) {
            try {
                Event event = EventDeserializer.deserialize(element.getBytes());
                return event.timestamp();
            } catch (Exception e) {
                LOG.warn("Could not extract timestamp from event, using record timestamp");
                return recordTimestamp;
            }
        }
    }
    
    /**
     * Aggregate function for counting events
     */
    private static class EventCountAggregator implements AggregateFunction<Event, EventCountAccumulator, Long> {
        @Override
        public EventCountAccumulator createAccumulator() {
            return new EventCountAccumulator();
        }
        
        @Override
        public EventCountAccumulator add(Event event, EventCountAccumulator accumulator) {
            accumulator.count++;
            accumulator.key = event.type();
            return accumulator;
        }
        
        @Override
        public Long getResult(EventCountAccumulator accumulator) {
            return accumulator.count;
        }
        
        @Override
        public EventCountAccumulator merge(EventCountAccumulator a, EventCountAccumulator b) {
            a.count += b.count;
            return a;
        }
    }
    
    /**
     * Aggregate function for user activity
     */
    private static class UserActivityAggregator implements AggregateFunction<Event, UserActivityAccumulator, UserActivity> {
        @Override
        public UserActivityAccumulator createAccumulator() {
            return new UserActivityAccumulator();
        }
        
        @Override
        public UserActivityAccumulator add(Event event, UserActivityAccumulator accumulator) {
            accumulator.userId = event.userId();
            accumulator.eventCount++;
            accumulator.firstEvent = Math.min(accumulator.firstEvent, event.timestamp());
            accumulator.lastEvent = Math.max(accumulator.lastEvent, event.timestamp());
            accumulator.eventTypes.add(event.type());
            return accumulator;
        }
        
        @Override
        public UserActivity getResult(UserActivityAccumulator accumulator) {
            return new UserActivity(
                    accumulator.userId,
                    accumulator.eventCount,
                    accumulator.eventTypes.size(),
                    accumulator.lastEvent - accumulator.firstEvent
            );
        }
        
        @Override
        public UserActivityAccumulator merge(UserActivityAccumulator a, UserActivityAccumulator b) {
            a.eventCount += b.eventCount;
            a.firstEvent = Math.min(a.firstEvent, b.firstEvent);
            a.lastEvent = Math.max(a.lastEvent, b.lastEvent);
            a.eventTypes.addAll(b.eventTypes);
            return a;
        }
    }
    
    /**
     * Session aggregator
     */
    private static class SessionAggregator implements AggregateFunction<Event, SessionAccumulator, SessionInfo> {
        @Override
        public SessionAccumulator createAccumulator() {
            return new SessionAccumulator();
        }
        
        @Override
        public SessionAccumulator add(Event event, SessionAccumulator accumulator) {
            accumulator.userId = event.userId();
            accumulator.eventCount++;
            accumulator.sessionStart = Math.min(accumulator.sessionStart, event.timestamp());
            accumulator.sessionEnd = Math.max(accumulator.sessionEnd, event.timestamp());
            accumulator.events.add(event.type());
            return accumulator;
        }
        
        @Override
        public SessionInfo getResult(SessionAccumulator accumulator) {
            return new SessionInfo(
                    accumulator.userId,
                    accumulator.eventCount,
                    accumulator.sessionEnd - accumulator.sessionStart,
                    accumulator.events.size()
            );
        }
        
        @Override
        public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
            a.eventCount += b.eventCount;
            a.sessionStart = Math.min(a.sessionStart, b.sessionStart);
            a.sessionEnd = Math.max(a.sessionEnd, b.sessionEnd);
            a.events.addAll(b.events);
            return a;
        }
    }
    
    /**
     * Window metadata function to add window information to results
     */
    private static class WindowMetadataFunction implements org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<Object, WindowResult, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Object> elements, org.apache.flink.util.Collector<WindowResult> out) {
            Object result = elements.iterator().next();
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            out.collect(new WindowResult(key, result, windowStart, windowEnd));
        }
    }
    
    // Accumulator classes
    private static class EventCountAccumulator {
        long count = 0;
        String key = "";
    }
    
    private static class UserActivityAccumulator {
        String userId = "";
        int eventCount = 0;
        long firstEvent = Long.MAX_VALUE;
        long lastEvent = Long.MIN_VALUE;
        java.util.Set<String> eventTypes = new java.util.HashSet<>();
    }
    
    private static class SessionAccumulator {
        String userId = "";
        int eventCount = 0;
        long sessionStart = Long.MAX_VALUE;
        long sessionEnd = Long.MIN_VALUE;
        java.util.Set<String> events = new java.util.HashSet<>();
    }
    
    // Result classes
    public record UserActivity(String userId, int eventCount, int uniqueEventTypes, long sessionDuration) {}
    public record SessionInfo(String userId, int eventCount, long sessionDuration, int uniqueEvents) {}
    public record WindowResult(String key, Object result, long windowStart, long windowEnd) {}
}
