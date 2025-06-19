package com.example.flink.cep;

import com.example.flink.data.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Complex Event Processing (CEP) with Apache Flink
 * 
 * Demonstrates advanced pattern detection including:
 * - Fraud detection patterns
 * - User behavior analysis
 * - System anomaly detection
 * - Temporal pattern matching
 */
public class AdvancedComplexEventPatternJob {
    
    private static final Logger logger = LoggerFactory.getLogger(AdvancedComplexEventPatternJob.class);
    
    public static void runDemo(StreamExecutionEnvironment env) throws Exception {
        logger.info("🔍 Starting Advanced Complex Event Processing Demo");
        
        // Create a stream of realistic events for pattern detection
        DataStream<Event> eventStream = env
            .fromElements(1L, 2L, 3L, 4L, 5L) // Simple elements to start
            .map(new EventMapFunction())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis())
            );
        
        // Pattern 1: Fraud Detection - Multiple high-value transactions in short time
        detectFraudPattern(eventStream);
        
        // Pattern 2: User Journey Analysis - Login -> Browse -> Purchase
        detectUserJourneyPattern(eventStream);
        
        // Pattern 3: System Anomaly Detection - Error spike patterns
        detectSystemAnomalyPattern(eventStream);
        
        // Pattern 4: Abandoned Cart Pattern
        detectAbandonedCartPattern(eventStream);
        
        logger.info("✅ Complex Event Processing patterns configured");
        
        // Execute the job
        env.execute("Advanced Complex Event Processing Job");
    }
    
    /**
     * Detect fraud patterns: Multiple high-value transactions in short time
     */
    private static void detectFraudPattern(DataStream<Event> eventStream) {
        logger.info("Setting up fraud detection pattern...");
        
        Pattern<Event, ?> fraudPattern = Pattern.<Event>begin("first")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "purchase".equals(event.eventType()) && event.getRevenueAmount() > 1000.0;
                }
            })
            .next("second")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "purchase".equals(event.eventType()) && event.getRevenueAmount() > 1000.0;
                }
            })
            .next("third")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "purchase".equals(event.eventType()) && event.getRevenueAmount() > 1000.0;
                }
            })
            .within(Time.minutes(10));
        
        PatternStream<Event> fraudPatternStream = CEP.pattern(
            eventStream.keyBy(Event::userId), 
            fraudPattern
        );
        
        DataStream<String> fraudAlerts = fraudPatternStream.select(
            new PatternSelectFunction<Event, String>() {
                @Override
                public String select(Map<String, List<Event>> pattern) throws Exception {
                    List<Event> firstEvents = pattern.get("first");
                    List<Event> secondEvents = pattern.get("second");
                    List<Event> thirdEvents = pattern.get("third");
                    
                    double totalAmount = 0.0;
                    totalAmount += firstEvents.get(0).getRevenueAmount();
                    totalAmount += secondEvents.get(0).getRevenueAmount();
                    totalAmount += thirdEvents.get(0).getRevenueAmount();
                    
                    Event firstEvent = firstEvents.get(0);
                    
                    return String.format(
                        "🚨 FRAUD ALERT: User %s made 3 high-value purchases (total: $%.2f) within 10 minutes",
                        firstEvent.userId(), totalAmount
                    );
                }
            }
        );
        
        fraudAlerts.print("FRAUD ALERT: ");
    }
    
    /**
     * Detect user journey patterns: Login -> Browse -> Purchase
     */
    private static void detectUserJourneyPattern(DataStream<Event> eventStream) {
        logger.info("Setting up user journey pattern...");
        
        Pattern<Event, ?> journeyPattern = Pattern.<Event>begin("login")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "login".equals(event.eventType());
                }
            })
            .followedBy("browse")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "page_view".equals(event.eventType());
                }
            })
            .oneOrMore()
            .greedy()
            .followedBy("purchase")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "purchase".equals(event.eventType());
                }
            })
            .within(Time.hours(2));
        
        PatternStream<Event> journeyPatternStream = CEP.pattern(
            eventStream.keyBy(Event::userId), 
            journeyPattern
        );
        
        DataStream<String> journeyAnalysis = journeyPatternStream.select(
            new PatternSelectFunction<Event, String>() {
                @Override
                public String select(Map<String, List<Event>> pattern) throws Exception {
                    Event login = pattern.get("login").get(0);
                    List<Event> browseEvents = pattern.get("browse");
                    Event purchase = pattern.get("purchase").get(0);
                    
                    long journeyTime = purchase.getEventTimeMillis() - login.getEventTimeMillis();
                    
                    return String.format(
                        "🛍️ SUCCESSFUL JOURNEY: User %s completed purchase after %d page views in %d minutes (amount: $%.2f)",
                        login.userId(), browseEvents.size(), journeyTime / 60000, purchase.getRevenueAmount()
                    );
                }
            }
        );
        
        journeyAnalysis.print("USER JOURNEY: ");
    }
    
    /**
     * Detect system anomaly patterns: Error spike detection
     */
    private static void detectSystemAnomalyPattern(DataStream<Event> eventStream) {
        logger.info("Setting up system anomaly detection pattern...");
        
        Pattern<Event, ?> anomalyPattern = Pattern.<Event>begin("errors")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "error".equals(event.eventType());
                }
            })
            .timesOrMore(3) // 3 or more errors
            .within(Time.minutes(1));
        
        PatternStream<Event> anomalyPatternStream = CEP.pattern(
            eventStream.keyBy(event -> "global"), // Global key for system-wide anomalies
            anomalyPattern
        );
        
        DataStream<String> anomalyAlerts = anomalyPatternStream.select(
            new PatternSelectFunction<Event, String>() {
                @Override
                public String select(Map<String, List<Event>> pattern) throws Exception {
                    List<Event> errors = pattern.get("errors");
                    
                    Map<String, Long> errorCodeCounts = errors.stream()
                        .collect(java.util.stream.Collectors.groupingBy(
                            event -> (String) event.properties().getOrDefault("errorCode", "unknown"),
                            java.util.stream.Collectors.counting()
                        ));
                    
                    return String.format(
                        "⚠️ SYSTEM ANOMALY: %d errors detected in 1 minute. Error codes: %s",
                        errors.size(), errorCodeCounts
                    );
                }
            }
        );
        
        anomalyAlerts.print("SYSTEM ANOMALY: ");
    }
    
    /**
     * Detect abandoned cart patterns: Add to cart followed by no purchase
     */
    private static void detectAbandonedCartPattern(DataStream<Event> eventStream) {
        logger.info("Setting up abandoned cart detection pattern...");
        
        Pattern<Event, ?> abandonedCartPattern = Pattern.<Event>begin("addToCart")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "add_to_cart".equals(event.eventType());
                }
            })
            .notFollowedBy("purchase")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return "purchase".equals(event.eventType());
                }
            })
            .within(Time.minutes(30));
        
        PatternStream<Event> abandonedCartStream = CEP.pattern(
            eventStream.keyBy(Event::userId), 
            abandonedCartPattern
        );
        
        DataStream<String> abandonedCartAlerts = abandonedCartStream.select(
            new PatternSelectFunction<Event, String>() {
                @Override
                public String select(Map<String, List<Event>> pattern) throws Exception {
                    Event addToCartEvent = pattern.get("addToCart").get(0);
                    String productId = (String) addToCartEvent.properties().getOrDefault("productId", "unknown");
                    
                    return String.format(
                        "🛒 ABANDONED CART: User %s added product %s to cart but didn't purchase within 30 minutes",
                        addToCartEvent.userId(), productId
                    );
                }
            }
        );
        
        abandonedCartAlerts.print("CART ANALYSIS: ");
    }
    
    /**
     * Map function to convert random numbers to realistic events
     */
    private static class EventMapFunction implements MapFunction<Long, Event> {
        
        private final Random random = new Random();
        private final String[] userIds = {"user1", "user2", "user3", "user4", "user5"};
        private final String[] productIds = {"product1", "product2", "product3", "product4", "product5"};
        private final String[] errorCodes = {"404", "500", "503", "timeout", "connection_error"};
        
        @Override
        public Event map(Long value) throws Exception {
            String userId = userIds[random.nextInt(userIds.length)];
            
            // Generate different event types with realistic patterns
            double eventTypeRandom = random.nextDouble();
            
            if (eventTypeRandom < 0.1) {
                // Login events
                return Event.Factory.login(userId, "web");
            } else if (eventTypeRandom < 0.4) {
                // Page view events
                return Event.Factory.pageView(userId, "product_page");
            } else if (eventTypeRandom < 0.5) {
                // Add to cart events
                String productId = productIds[random.nextInt(productIds.length)];
                return Event.Factory.addToCart(userId, productId, 1, 99.99);
            } else if (eventTypeRandom < 0.6) {
                // Purchase events (some high-value for fraud detection)
                String productId = productIds[random.nextInt(productIds.length)];
                double amount = random.nextBoolean() ? 
                    random.nextDouble() * 500 + 50 :  // Normal purchase
                    random.nextDouble() * 2000 + 1000; // High-value purchase
                return Event.Factory.purchase(userId, amount, productId);
            } else if (eventTypeRandom < 0.65) {
                // Error events (sometimes in bursts for anomaly detection)
                String errorCode = errorCodes[random.nextInt(errorCodes.length)];
                return Event.Factory.error(userId, errorCode, "Test error message");
            } else {
                // Logout events
                return Event.Factory.logout(userId);
            }
        }
    }
}
