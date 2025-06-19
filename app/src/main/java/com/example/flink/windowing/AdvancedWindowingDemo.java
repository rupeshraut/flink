package com.example.flink.windowing;

import com.example.flink.data.Event;
import com.example.flink.utils.SyntheticEventGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Advanced Windowing Strategies Demo
 * 
 * Demonstrates various windowing patterns in Flink:
 * - Tumbling windows for batch analytics
 * - Sliding windows for real-time metrics
 * - Session windows for user behavior analysis
 * - Custom window triggers and evictors
 * - Late data handling strategies
 */
public class AdvancedWindowingDemo {
    
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedWindowingDemo.class);
    
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        LOG.info("🪟 Starting Advanced Windowing Demo");
        
        // Create event stream with realistic data
        DataStream<Event> eventStream = env
            .addSource(new RealisticEventSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                    .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis())
                    .withIdleness(Duration.ofMinutes(5))
            )
            .name("event-source");

        // Demo 1: Tumbling Windows - Hourly Sales Analytics
        demonstrateTumblingWindows(eventStream);
        
        // Demo 2: Sliding Windows - Real-time Moving Averages
        demonstrateSlidingWindows(eventStream);
        
        // Demo 3: Session Windows - User Behavior Sessions
        demonstrateSessionWindows(eventStream);
        
        // Demo 4: Custom Window Triggers - Early Results
        demonstrateCustomTriggers(eventStream);
        
        // Demo 5: Global Windows with Custom Triggers
        demonstrateGlobalWindows(eventStream);
        
        LOG.info("✅ Advanced windowing strategies configured");
        
        // Execute the job
        env.execute("Advanced Windowing Demo");
    }
    
    /**
     * Demonstrate tumbling windows for batch analytics
     */
    private static void demonstrateTumblingWindows(DataStream<Event> eventStream) {
        LOG.info("Setting up tumbling windows for hourly analytics...");
        
        eventStream
            .filter(event -> "purchase".equals(event.eventType()))
            .keyBy(event -> event.properties().getOrDefault("category", "unknown"))
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .aggregate(new SalesAggregator(), new SalesWindowProcessor())
            .name("hourly-sales-analytics")
            .print("HOURLY SALES: ");
    }
    
    /**
     * Demonstrate sliding windows for real-time metrics
     */
    private static void demonstrateSlidingWindows(DataStream<Event> eventStream) {
        LOG.info("Setting up sliding windows for real-time moving averages...");
        
        eventStream
            .filter(event -> event.isRevenueEvent())
            .keyBy(Event::userId)
            .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
            .aggregate(new RevenueMovingAverageAggregator())
            .name("revenue-moving-average")
            .print("MOVING AVERAGE: ");
    }
    
    /**
     * Demonstrate session windows for user behavior analysis
     */
    private static void demonstrateSessionWindows(DataStream<Event> eventStream) {
        LOG.info("Setting up session windows for user behavior analysis...");
        
        eventStream
            .keyBy(Event::userId)
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .aggregate(new UserSessionAggregator(), new SessionWindowProcessor())
            .name("user-session-analysis")
            .print("USER SESSION: ");
    }
    
    /**
     * Demonstrate custom triggers for early window results
     */
    private static void demonstrateCustomTriggers(DataStream<Event> eventStream) {
        LOG.info("Setting up custom triggers for early results...");
        
        eventStream
            .filter(event -> "page_view".equals(event.eventType()))
            .keyBy(event -> event.properties().getOrDefault("page", "unknown"))
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.seconds(30))))
            .aggregate(new PageViewAggregator())
            .name("early-page-view-metrics")
            .print("EARLY PAGE VIEWS: ");
    }
    
    /**
     * Demonstrate global windows with count-based triggers
     */
    private static void demonstrateGlobalWindows(DataStream<Event> eventStream) {
        LOG.info("Setting up global windows with count-based processing...");
        
        eventStream
            .filter(event -> "error".equals(event.eventType()))
            .keyBy(event -> event.properties().getOrDefault("errorCode", "unknown"))
            .window(GlobalWindows.create())
            .trigger(PurgingTrigger.of(org.apache.flink.streaming.api.windowing.triggers.CountTrigger.of(5)))
            .reduce(new ErrorEventReducer())
            .name("error-count-analysis")
            .print("ERROR ANALYSIS: ");
    }
    
    /**
     * Sales aggregator for tumbling windows
     */
    private static class SalesAggregator implements AggregateFunction<Event, SalesAccumulator, SalesResult> {
        
        @Override
        public SalesAccumulator createAccumulator() {
            return new SalesAccumulator();
        }
        
        @Override
        public SalesAccumulator add(Event event, SalesAccumulator accumulator) {
            accumulator.totalRevenue += event.getRevenueAmount();
            accumulator.transactionCount++;
            accumulator.maxTransaction = Math.max(accumulator.maxTransaction, event.getRevenueAmount());
            accumulator.minTransaction = Math.min(accumulator.minTransaction, event.getRevenueAmount());
            return accumulator;
        }
        
        @Override
        public SalesResult getResult(SalesAccumulator accumulator) {
            double avgTransaction = accumulator.transactionCount > 0 ? 
                accumulator.totalRevenue / accumulator.transactionCount : 0.0;
            return new SalesResult(
                accumulator.totalRevenue,
                accumulator.transactionCount,
                avgTransaction,
                accumulator.maxTransaction,
                accumulator.minTransaction
            );
        }
        
        @Override
        public SalesAccumulator merge(SalesAccumulator a, SalesAccumulator b) {
            a.totalRevenue += b.totalRevenue;
            a.transactionCount += b.transactionCount;
            a.maxTransaction = Math.max(a.maxTransaction, b.maxTransaction);
            a.minTransaction = Math.min(a.minTransaction, b.minTransaction);
            return a;
        }
    }
    
    /**
     * Window processor for sales analytics
     */
    private static class SalesWindowProcessor extends ProcessWindowFunction<SalesResult, Tuple4<String, Long, Long, SalesResult>, String, TimeWindow> {
        @Override
        public void process(String category, Context context, Iterable<SalesResult> elements, Collector<Tuple4<String, Long, Long, SalesResult>> out) {
            SalesResult result = elements.iterator().next();
            out.collect(new Tuple4<>(category, context.window().getStart(), context.window().getEnd(), result));
        }
    }
    
    /**
     * Revenue moving average aggregator
     */
    private static class RevenueMovingAverageAggregator implements AggregateFunction<Event, MovingAverageAccumulator, Double> {
        
        @Override
        public MovingAverageAccumulator createAccumulator() {
            return new MovingAverageAccumulator();
        }
        
        @Override
        public MovingAverageAccumulator add(Event event, MovingAverageAccumulator accumulator) {
            accumulator.sum += event.getRevenueAmount();
            accumulator.count++;
            return accumulator;
        }
        
        @Override
        public Double getResult(MovingAverageAccumulator accumulator) {
            return accumulator.count > 0 ? accumulator.sum / accumulator.count : 0.0;
        }
        
        @Override
        public MovingAverageAccumulator merge(MovingAverageAccumulator a, MovingAverageAccumulator b) {
            a.sum += b.sum;
            a.count += b.count;
            return a;
        }
    }
    
    /**
     * User session aggregator
     */
    private static class UserSessionAggregator implements AggregateFunction<Event, UserSessionAccumulator, UserSessionResult> {
        
        @Override
        public UserSessionAccumulator createAccumulator() {
            return new UserSessionAccumulator();
        }
        
        @Override
        public UserSessionAccumulator add(Event event, UserSessionAccumulator accumulator) {
            accumulator.eventCount++;
            accumulator.totalValue += event.value();
            
            if ("purchase".equals(event.eventType())) {
                accumulator.purchaseCount++;
                accumulator.totalRevenue += event.getRevenueAmount();
            }
            
            if (accumulator.firstEventTime == 0) {
                accumulator.firstEventTime = event.getEventTimeMillis();
            }
            accumulator.lastEventTime = event.getEventTimeMillis();
            
            return accumulator;
        }
        
        @Override
        public UserSessionResult getResult(UserSessionAccumulator accumulator) {
            long sessionDuration = accumulator.lastEventTime - accumulator.firstEventTime;
            return new UserSessionResult(
                accumulator.eventCount,
                accumulator.purchaseCount,
                accumulator.totalRevenue,
                sessionDuration,
                accumulator.firstEventTime,
                accumulator.lastEventTime
            );
        }
        
        @Override
        public UserSessionAccumulator merge(UserSessionAccumulator a, UserSessionAccumulator b) {
            a.eventCount += b.eventCount;
            a.purchaseCount += b.purchaseCount;
            a.totalRevenue += b.totalRevenue;
            a.totalValue += b.totalValue;
            a.firstEventTime = Math.min(a.firstEventTime, b.firstEventTime);
            a.lastEventTime = Math.max(a.lastEventTime, b.lastEventTime);
            return a;
        }
    }
    
    /**
     * Session window processor
     */
    private static class SessionWindowProcessor extends ProcessWindowFunction<UserSessionResult, Tuple3<String, TimeWindow, UserSessionResult>, String, TimeWindow> {
        @Override
        public void process(String userId, Context context, Iterable<UserSessionResult> elements, Collector<Tuple3<String, TimeWindow, UserSessionResult>> out) {
            UserSessionResult result = elements.iterator().next();
            out.collect(new Tuple3<>(userId, context.window(), result));
        }
    }
    
    /**
     * Page view aggregator for early triggers
     */
    private static class PageViewAggregator implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }
        
        @Override
        public Long add(Event event, Long accumulator) {
            return accumulator + 1;
        }
        
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }
        
        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
    
    /**
     * Error event reducer for global windows
     */
    private static class ErrorEventReducer implements ReduceFunction<Event> {
        @Override
        public Event reduce(Event event1, Event event2) throws Exception {
            // Combine error events into a summary
            return new Event(
                "error-summary-" + System.currentTimeMillis(),
                "system",
                "error_summary",
                2.0, // Count of errors
                Instant.now(),
                java.util.Map.of(
                    "errorCode", event1.properties().get("errorCode"),
                    "lastError", event2.properties().get("message"),
                    "count", 2
                )
            );
        }
    }
    
    /**
     * Realistic event source for demonstration
     */
    private static class RealisticEventSource implements SourceFunction<Event> {
        
        private volatile boolean running = true;
        private final SyntheticEventGenerator generator = new SyntheticEventGenerator(100);
        
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                Event event = generator.generateEvent();
                ctx.collect(event);
                
                // Simulate realistic event rates with some variation
                Thread.sleep(ThreadLocalRandom.current().nextInt(50, 200));
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
    
    // Data classes for accumulators and results
    
    public static class SalesAccumulator {
        double totalRevenue = 0.0;
        long transactionCount = 0;
        double maxTransaction = Double.MIN_VALUE;
        double minTransaction = Double.MAX_VALUE;
    }
    
    public record SalesResult(double totalRevenue, long transactionCount, double avgTransaction, double maxTransaction, double minTransaction) {}
    
    public static class MovingAverageAccumulator {
        double sum = 0.0;
        long count = 0;
    }
    
    public static class UserSessionAccumulator {
        long eventCount = 0;
        long purchaseCount = 0;
        double totalRevenue = 0.0;
        double totalValue = 0.0;
        long firstEventTime = 0;
        long lastEventTime = 0;
    }
    
    public record UserSessionResult(long eventCount, long purchaseCount, double totalRevenue, long sessionDuration, long startTime, long endTime) {}
}
