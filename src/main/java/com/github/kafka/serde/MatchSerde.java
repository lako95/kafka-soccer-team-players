package com.github.kafka.serde;

import com.github.kafka.deserializer.MatchDeserializer;
import com.github.kafka.serializer.MatchSerializer;
import com.github.soccer.dto.Match;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MatchSerde implements Serde<Match> {

    private MatchSerializer matchSerializer = new MatchSerializer();
    private MatchDeserializer matchDeserializer = new MatchDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {
        matchSerializer.close();
        matchDeserializer.close();
    }

    @Override
    public Serializer<Match> serializer() {
        return matchSerializer;
    }

    @Override
    public Deserializer<Match> deserializer() {
        return matchDeserializer;
    }
}
