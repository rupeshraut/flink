package com.example.flink.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;

import java.time.Instant;
import java.util.Objects;

/**
 * Event data class representing a business event in the stream
 * 
 * This record represents events flowing through our Kafka/Flink pipeline
 * with proper serialization support and event-time handling.
 */
public record Event(
    @JsonProperty("eventId") String eventId,
    @JsonProperty("userId") String userId,
    @JsonProperty("eventType") String eventType,
    @JsonProperty("value") double value,
    @JsonProperty("timestamp") 
    @JsonSerialize(using = InstantSerializer.class)
    @JsonDeserialize(using = InstantDeserializer.class)
    Instant timestamp,
    @JsonProperty("properties") java.util.Map<String, Object> properties
) {
    
    /**
     * Constructor with default properties
     */
    public Event(String eventId, String userId, String eventType, double value, Instant timestamp) {
        this(eventId, userId, eventType, value, timestamp, new java.util.HashMap<>());
    }
    
    /**
     * Constructor with current timestamp
     */
    public Event(String eventId, String userId, String eventType, double value) {
        this(eventId, userId, eventType, value, Instant.now(), new java.util.HashMap<>());
    }
    
    /**
     * Get event time for Flink watermark assignment
     */
    public long getEventTimeMillis() {
        return timestamp.toEpochMilli();
    }
    
    /**
     * Check if event is valid for processing
     */
    public boolean isValid() {
        return eventId != null && !eventId.trim().isEmpty() &&
               userId != null && !userId.trim().isEmpty() &&
               eventType != null && !eventType.trim().isEmpty() &&
               !Double.isNaN(value) && Double.isFinite(value) &&
               timestamp != null;
    }
    
    /**
     * Check if this is a revenue-generating event
     */
    public boolean isRevenueEvent() {
        return "purchase".equals(eventType) || "subscription".equals(eventType);
    }
    
    /**
     * Get revenue amount if this is a revenue event
     */
    public double getRevenueAmount() {
        if (!isRevenueEvent()) {
            return 0.0;
        }
        Object amount = properties.get("amount");
        if (amount instanceof Number) {
            return ((Number) amount).doubleValue();
        }
        return value;
    }
    
    /**
     * Create a copy with updated properties
     */
    public Event withProperties(java.util.Map<String, Object> newProperties) {
        return new Event(eventId, userId, eventType, value, timestamp, newProperties);
    }
    
    /**
     * Create a copy with an additional property
     */
    public Event withProperty(String key, Object value) {
        java.util.Map<String, Object> newProperties = new java.util.HashMap<>(this.properties);
        newProperties.put(key, value);
        return new Event(eventId, userId, eventType, this.value, timestamp, newProperties);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Double.compare(event.value, value) == 0 &&
               Objects.equals(eventId, event.eventId) &&
               Objects.equals(userId, event.userId) &&
               Objects.equals(eventType, event.eventType) &&
               Objects.equals(timestamp, event.timestamp) &&
               Objects.equals(properties, event.properties);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(eventId, userId, eventType, value, timestamp, properties);
    }
    
    @Override
    public String toString() {
        return "Event{" +
               "eventId='" + eventId + '\'' +
               ", userId='" + userId + '\'' +
               ", eventType='" + eventType + '\'' +
               ", value=" + value +
               ", timestamp=" + timestamp +
               ", properties=" + properties +
               '}';
    }
    
    /**
     * Factory methods for common event types
     */
    public static class Factory {
        
        public static Event pageView(String userId, String page) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "page_view",
                1.0,
                Instant.now(),
                java.util.Map.of("page", page, "category", "engagement")
            );
        }
        
        public static Event purchase(String userId, double amount, String productId) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "purchase",
                amount,
                Instant.now(),
                java.util.Map.of("productId", productId, "amount", amount, "currency", "USD")
            );
        }
        
        public static Event purchase(String userId, double amount, String productId, String category) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "purchase",
                amount,
                Instant.now(),
                java.util.Map.of(
                    "productId", productId, 
                    "amount", amount, 
                    "currency", "USD",
                    "category", category
                )
            );
        }
        
        public static Event login(String userId, String source) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "login",
                1.0,
                Instant.now(),
                java.util.Map.of("source", source, "category", "authentication")
            );
        }
        
        public static Event logout(String userId) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "logout",
                1.0,
                Instant.now(),
                java.util.Map.of("category", "authentication")
            );
        }
        
        public static Event addToCart(String userId, String productId, int quantity, double price) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "add_to_cart",
                price * quantity,
                Instant.now(),
                java.util.Map.of(
                    "productId", productId,
                    "quantity", quantity,
                    "price", price,
                    "category", "commerce"
                )
            );
        }
        
        public static Event removeFromCart(String userId, String productId, int quantity) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "remove_from_cart",
                0.0,
                Instant.now(),
                java.util.Map.of(
                    "productId", productId,
                    "quantity", quantity,
                    "category", "commerce"
                )
            );
        }
        
        public static Event error(String userId, String errorCode, String message) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "error",
                0.0,
                Instant.now(),
                java.util.Map.of(
                    "errorCode", errorCode, 
                    "message", message, 
                    "severity", "error",
                    "category", "system"
                )
            );
        }
        
        public static Event warning(String userId, String warningCode, String message) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "warning",
                0.0,
                Instant.now(),
                java.util.Map.of(
                    "warningCode", warningCode, 
                    "message", message, 
                    "severity", "warning",
                    "category", "system"
                )
            );
        }
        
        public static Event sessionStart(String userId, String sessionId) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "session_start",
                1.0,
                Instant.now(),
                java.util.Map.of(
                    "sessionId", sessionId,
                    "category", "session"
                )
            );
        }
        
        public static Event sessionEnd(String userId, String sessionId, long durationMs) {
            return new Event(
                java.util.UUID.randomUUID().toString(),
                userId,
                "session_end",
                1.0,
                Instant.now(),
                java.util.Map.of(
                    "sessionId", sessionId,
                    "durationMs", durationMs,
                    "category", "session"
                )
            );
        }
    }
}
