package com.github.kafka;

import com.github.kafka.serializer.MatchSerializer;
import com.github.soccer.dto.Match;
import com.github.soccer.rest.MatchServiceClient;
import com.github.utils.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Producer.class.getName());

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MatchSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MatchSerializer.class.getName());

        //create the producer
        KafkaProducer<String,Match> producer = new KafkaProducer<String, Match>(properties);

        List<Match> matches = MatchServiceClient.getMatchesFromApi();
        logger.info("N matches fetched = " + matches.size());
        if(matches != null) {
            //send data
            for (Match match : matches) {
                ProducerRecord<String, Match> record = new ProducerRecord<String, Match>(Constants.SOCCER_TOPIC, match);
                //asyncronous
                producer.send(record);
            }

            //flush data
            producer.flush();
            //flush and close
            producer.close();
        }

    }
}
