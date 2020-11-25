package com.github.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class MatchSerializer implements org.apache.kafka.common.serialization.Serializer {

    public void configure(Map map, boolean b) {

    }

    public byte[] serialize(String s, Object o) {

        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(o).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    public void close() {

    }
}