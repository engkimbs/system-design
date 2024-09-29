package com.engkimbs.datacollection.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> targetClass;

    public JsonDeserializer() {
    }

    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (targetClass == null) {
            String className = (String) configs.get("spring.json.value.default.type");
            try {
                this.targetClass = (Class<T>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize JSON message", e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                return null;
            }
            return objectMapper.readValue(data, targetClass);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
