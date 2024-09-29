package com.engkimbs.datacollection.serializer;

import org.apache.kafka.common.serialization.Serdes;

public class JsonSerde<T> extends Serdes.WrapperSerde<T> {

    public JsonSerde(Class<T> targetClass) {
        super(new JsonSerializer<>(), new JsonDeserializer<>(targetClass));
    }

    public JsonSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>());
    }
}