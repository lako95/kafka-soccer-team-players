package com.github.kafka;

import com.github.kafka.deserializer.MatchDeserializer;
import com.github.kafka.serde.MatchSerde;
import com.github.soccer.dto.Match;
import com.github.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStream {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaStream.class.getName());

        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "soccer-team-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MatchDeserializer.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MatchSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, Match> inputTopic = streamsBuilder.stream(Constants.SOCCER_TOPIC);

        KStream<String, Match> filteredStream = inputTopic.filter(
                (k,v) -> v.getWinner().equals("NONE")
        );

        filteredStream.to(Constants.NO_WINNER_TOPIC);

        //build the topology
        KafkaStreams kafkaStream = new KafkaStreams(streamsBuilder.build(),properties);
        kafkaStream.start();
    }
}
