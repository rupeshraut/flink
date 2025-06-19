package com.example.flink.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Custom deserializer for Event objects from Kafka
 * 
 * Handles JSON deserialization with proper error handling and logging
 */
public class EventDeserializer implements DeserializationSchema<Event> {
    
    private static final Logger logger = LoggerFactory.getLogger(EventDeserializer.class);
    
    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());
    
    @Override
    public Event deserialize(byte[] message) throws IOException {
        try {
            if (message == null || message.length == 0) {
                logger.warn("Received empty message, skipping deserialization");
                return null;
            }
            
            String jsonString = new String(message);
            Event event = objectMapper.readValue(jsonString, Event.class);
            
            // Validate the event
            if (!event.isValid()) {
                logger.warn("Invalid event received: {}", event);
                return null;
            }
            
            return event;
            
        } catch (Exception e) {
            logger.error("Failed to deserialize event from message: {}", new String(message), e);
            throw new IOException("Event deserialization failed", e);
        }
    }
    
    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false; // We process infinite streams
    }
    
    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
