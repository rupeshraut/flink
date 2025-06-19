package com.example.flink.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Custom serializer for Event objects to Kafka
 * 
 * Handles JSON serialization with proper error handling and logging
 */
public class EventSerializer implements SerializationSchema<Event> {
    
    private static final Logger logger = LoggerFactory.getLogger(EventSerializer.class);
    
    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());
    
    @Override
    public byte[] serialize(Event event) {
        try {
            if (event == null) {
                logger.warn("Attempting to serialize null event");
                return new byte[0];
            }
            
            if (!event.isValid()) {
                logger.warn("Attempting to serialize invalid event: {}", event);
                return new byte[0];
            }
            
            String jsonString = objectMapper.writeValueAsString(event);
            return jsonString.getBytes();
            
        } catch (Exception e) {
            logger.error("Failed to serialize event: {}", event, e);
            throw new RuntimeException("Event serialization failed", e);
        }
    }
}
