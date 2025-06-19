package com.example.flink.utils;

import com.example.flink.data.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Synthetic event generator for testing and demonstration purposes.
 * Generates realistic event streams with various patterns:
 * - User behavior patterns
 * - E-commerce events
 * - Time-based variations
 * - Anomalies and outliers
 * - Seasonal patterns
 */
public class SyntheticEventGenerator {
    
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticEventGenerator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    // Event types and their relative frequencies
    private static final Map<String, Double> EVENT_TYPE_WEIGHTS = Map.of(
            "view", 0.4,
            "click", 0.25,
            "add_to_cart", 0.15,
            "remove_from_cart", 0.05,
            "purchase", 0.08,
            "login", 0.04,
            "logout", 0.03
    );
    
    // Product categories
    private static final String[] PRODUCT_CATEGORIES = {
            "electronics", "clothing", "books", "home", "sports", "toys", "food", "beauty"
    };
    
    // Device types
    private static final String[] DEVICE_TYPES = {
            "mobile", "desktop", "tablet"
    };
    
    // User segments
    private static final String[] USER_SEGMENTS = {
            "premium", "regular", "new", "churned"
    };
    
    private final Random random;
    private final List<String> userIds;
    private final Map<String, UserProfile> userProfiles;
    
    public SyntheticEventGenerator(int numUsers, long seed) {
        this.random = new Random(seed);
        this.userIds = generateUserIds(numUsers);
        this.userProfiles = generateUserProfiles();
        LOG.info("Initialized synthetic event generator with {} users", numUsers);
    }
    
    public SyntheticEventGenerator(int numUsers) {
        this(numUsers, System.currentTimeMillis());
    }
    
    /**
     * Generate a single random event
     */
    public Event generateEvent() {
        String userId = selectRandomUser();
        UserProfile profile = userProfiles.get(userId);
        String eventType = selectEventType(profile);
        long timestamp = generateTimestamp();
        Map<String, Object> data = generateEventData(eventType, profile);
        
        return Event.builder()
                .userId(userId)
                .type(eventType)
                .timestamp(timestamp)
                .data(data)
                .build();
    }
    
    /**
     * Generate a batch of events
     */
    public List<Event> generateEvents(int count) {
        List<Event> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            events.add(generateEvent());
        }
        return events;
    }
    
    /**
     * Generate events for a specific user session
     */
    public List<Event> generateUserSession(String userId) {
        UserProfile profile = userProfiles.get(userId);
        if (profile == null) {
            profile = new UserProfile(userId, "regular", "desktop", 0.5);
        }
        
        List<Event> sessionEvents = new ArrayList<>();
        long sessionStart = System.currentTimeMillis();
        long currentTime = sessionStart;
        
        // Session length: 5-60 minutes
        long sessionDuration = (5 + random.nextInt(55)) * 60 * 1000;
        long sessionEnd = sessionStart + sessionDuration;
        
        // Generate session events
        while (currentTime < sessionEnd && sessionEvents.size() < 50) {
            String eventType = selectEventType(profile);
            Map<String, Object> data = generateEventData(eventType, profile);
            
            Event event = Event.builder()
                    .userId(userId)
                    .type(eventType)
                    .timestamp(currentTime)
                    .data(data)
                    .build();
            
            sessionEvents.add(event);
            
            // Time between events: 10 seconds to 5 minutes
            currentTime += (10 + random.nextInt(290)) * 1000;
        }
        
        return sessionEvents;
    }
    
    /**
     * Generate events with specific patterns for testing
     */
    public List<Event> generatePatternEvents(EventPattern pattern, String userId) {
        List<Event> events = new ArrayList<>();
        long baseTime = System.currentTimeMillis();
        
        switch (pattern) {
            case PURCHASE_FUNNEL:
                events.add(createEvent(userId, "view", baseTime, createViewData()));
                events.add(createEvent(userId, "click", baseTime + 30000, createClickData()));
                events.add(createEvent(userId, "add_to_cart", baseTime + 120000, createCartData()));
                events.add(createEvent(userId, "purchase", baseTime + 300000, createPurchaseData()));
                break;
                
            case ABANDONED_CART:
                events.add(createEvent(userId, "view", baseTime, createViewData()));
                events.add(createEvent(userId, "add_to_cart", baseTime + 60000, createCartData()));
                events.add(createEvent(userId, "add_to_cart", baseTime + 120000, createCartData()));
                // No purchase event - cart abandoned
                break;
                
            case BROWSING_SESSION:
                for (int i = 0; i < 10; i++) {
                    events.add(createEvent(userId, "view", baseTime + i * 30000, createViewData()));
                    if (i % 3 == 0) {
                        events.add(createEvent(userId, "click", baseTime + i * 30000 + 15000, createClickData()));
                    }
                }
                break;
                
            case HIGH_VALUE_PURCHASE:
                events.add(createEvent(userId, "view", baseTime, createViewData()));
                events.add(createEvent(userId, "purchase", baseTime + 180000, createHighValuePurchaseData()));
                break;
                
            case RAPID_ACTIVITY:
                for (int i = 0; i < 20; i++) {
                    String eventType = i % 2 == 0 ? "view" : "click";
                    events.add(createEvent(userId, eventType, baseTime + i * 5000, createViewData()));
                }
                break;
        }
        
        return events;
    }
    
    /**
     * Generate user IDs
     */
    private List<String> generateUserIds(int numUsers) {
        List<String> ids = new ArrayList<>(numUsers);
        for (int i = 0; i < numUsers; i++) {
            ids.add("user_" + String.format("%06d", i));
        }
        return ids;
    }
    
    /**
     * Generate user profiles with different behaviors
     */
    private Map<String, UserProfile> generateUserProfiles() {
        Map<String, UserProfile> profiles = new HashMap<>();
        
        for (String userId : userIds) {
            String segment = USER_SEGMENTS[random.nextInt(USER_SEGMENTS.length)];
            String device = DEVICE_TYPES[random.nextInt(DEVICE_TYPES.length)];
            double activityLevel = 0.1 + random.nextDouble() * 0.9; // 0.1 to 1.0
            
            profiles.put(userId, new UserProfile(userId, segment, device, activityLevel));
        }
        
        return profiles;
    }
    
    /**
     * Select a random user based on activity levels
     */
    private String selectRandomUser() {
        // Weight selection by user activity level
        double totalWeight = userProfiles.values().stream()
                .mapToDouble(profile -> profile.activityLevel)
                .sum();
        
        double randomValue = random.nextDouble() * totalWeight;
        double currentWeight = 0;
        
        for (UserProfile profile : userProfiles.values()) {
            currentWeight += profile.activityLevel;
            if (randomValue <= currentWeight) {
                return profile.userId;
            }
        }
        
        // Fallback
        return userIds.get(random.nextInt(userIds.size()));
    }
    
    /**
     * Select event type based on user profile and weights
     */
    private String selectEventType(UserProfile profile) {
        Map<String, Double> adjustedWeights = new HashMap<>(EVENT_TYPE_WEIGHTS);
        
        // Adjust weights based on user segment
        switch (profile.segment) {
            case "premium":
                adjustedWeights.put("purchase", adjustedWeights.get("purchase") * 2.0);
                adjustedWeights.put("view", adjustedWeights.get("view") * 1.2);
                break;
            case "new":
                adjustedWeights.put("login", adjustedWeights.get("login") * 3.0);
                adjustedWeights.put("view", adjustedWeights.get("view") * 1.5);
                break;
            case "churned":
                adjustedWeights.put("logout", adjustedWeights.get("logout") * 2.0);
                adjustedWeights.put("purchase", adjustedWeights.get("purchase") * 0.3);
                break;
        }
        
        return selectWeightedRandom(adjustedWeights);
    }
    
    /**
     * Select random item based on weights
     */
    private String selectWeightedRandom(Map<String, Double> weights) {
        double totalWeight = weights.values().stream().mapToDouble(Double::doubleValue).sum();
        double randomValue = random.nextDouble() * totalWeight;
        double currentWeight = 0;
        
        for (Map.Entry<String, Double> entry : weights.entrySet()) {
            currentWeight += entry.getValue();
            if (randomValue <= currentWeight) {
                return entry.getKey();
            }
        }
        
        // Fallback
        return weights.keySet().iterator().next();
    }
    
    /**
     * Generate timestamp with some variability
     */
    private long generateTimestamp() {
        // Current time +/- 1 hour
        long now = System.currentTimeMillis();
        long variance = random.nextInt(2 * 60 * 60 * 1000) - (60 * 60 * 1000);
        return now + variance;
    }
    
    /**
     * Generate event-specific data
     */
    private Map<String, Object> generateEventData(String eventType, UserProfile profile) {
        Map<String, Object> data = new HashMap<>();
        data.put("device", profile.device);
        data.put("segment", profile.segment);
        
        switch (eventType) {
            case "view":
                data.putAll(createViewData());
                break;
            case "click":
                data.putAll(createClickData());
                break;
            case "add_to_cart":
            case "remove_from_cart":
                data.putAll(createCartData());
                break;
            case "purchase":
                data.putAll(createPurchaseData());
                break;
            case "login":
            case "logout":
                data.put("session_id", "session_" + random.nextInt(10000));
                break;
        }
        
        return data;
    }
    
    private Map<String, Object> createViewData() {
        Map<String, Object> data = new HashMap<>();
        data.put("product_id", "product_" + random.nextInt(1000));
        data.put("category", PRODUCT_CATEGORIES[random.nextInt(PRODUCT_CATEGORIES.length)]);
        data.put("page", "product_detail");
        data.put("duration", 10 + random.nextInt(300)); // 10-310 seconds
        return data;
    }
    
    private Map<String, Object> createClickData() {
        Map<String, Object> data = new HashMap<>();
        data.put("element", random.nextBoolean() ? "product_image" : "add_to_cart_button");
        data.put("page", "product_detail");
        return data;
    }
    
    private Map<String, Object> createCartData() {
        Map<String, Object> data = new HashMap<>();
        data.put("product_id", "product_" + random.nextInt(1000));
        data.put("quantity", 1 + random.nextInt(5));
        data.put("price", 10.0 + random.nextDouble() * 500.0);
        return data;
    }
    
    private Map<String, Object> createPurchaseData() {
        Map<String, Object> data = new HashMap<>();
        data.put("order_id", "order_" + random.nextInt(100000));
        data.put("amount", 20.0 + random.nextDouble() * 300.0);
        data.put("currency", "USD");
        data.put("payment_method", random.nextBoolean() ? "credit_card" : "paypal");
        data.put("items", 1 + random.nextInt(5));
        return data;
    }
    
    private Map<String, Object> createHighValuePurchaseData() {
        Map<String, Object> data = createPurchaseData();
        data.put("amount", 500.0 + random.nextDouble() * 2000.0); // $500-$2500
        return data;
    }
    
    private Event createEvent(String userId, String type, long timestamp, Map<String, Object> data) {
        return Event.builder()
                .userId(userId)
                .type(type)
                .timestamp(timestamp)
                .data(data)
                .build();
    }
    
    // Inner classes
    private static class UserProfile {
        final String userId;
        final String segment;
        final String device;
        final double activityLevel;
        
        UserProfile(String userId, String segment, String device, double activityLevel) {
            this.userId = userId;
            this.segment = segment;
            this.device = device;
            this.activityLevel = activityLevel;
        }
    }
    
    public enum EventPattern {
        PURCHASE_FUNNEL,
        ABANDONED_CART,
        BROWSING_SESSION,
        HIGH_VALUE_PURCHASE,
        RAPID_ACTIVITY
    }
}
