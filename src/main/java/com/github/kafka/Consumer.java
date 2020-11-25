package com.github.kafka;

import com.github.db.DbManager;
import com.github.soccer.dto.Match;
import com.github.utils.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
        String groupId = "consumer-app";

        //create customer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "com.github.kafka.deserializer.MatchDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.github.kafka.deserializer.MatchDeserializer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String,Match> consumer = new KafkaConsumer<String, Match>(properties);

        //subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(Constants.SOCCER_TOPIC));

        //poll for new data
        while(true){
            ConsumerRecords<String, Match> records = consumer.poll(Duration.ofMillis(100));
            List<Match> matchList = new ArrayList<>();
            for(ConsumerRecord record: records){
                Match match = (Match) record.value();
                matchList.add(match);
            }

            try {
                logger.info("found " + matchList.size() + " match to insert");
                DbManager.insertMatches(matchList);
                Thread.sleep(10000);
            } catch (Exception e) {
                logger.error("error",e);
            }
        }
    }
}
