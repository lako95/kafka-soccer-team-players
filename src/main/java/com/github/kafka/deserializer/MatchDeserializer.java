package com.github.kafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.soccer.dto.Match;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class MatchDeserializer implements  org.apache.kafka.common.serialization.Deserializer {

     public void close() {

    }

    public void configure(Map map, boolean b) {

    }


    public Match deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        Match match = null;
        try {
            match = mapper.readValue(arg1, Match.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return match;
    }

}