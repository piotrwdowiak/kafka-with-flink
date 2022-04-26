package com.piotrwdowiak.kafkawithflink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaAvroProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroProducer.class);


    @Autowired
    private KafkaTemplate<String, com.piotrwdowiak.kafkawithflink.Thing> kafkaTemplate;

    void send(String topic, com.piotrwdowiak.kafkawithflink.Thing thing) {
        this.kafkaTemplate.send(topic, thing);
        LOGGER.info(String.format("Produced user -> %s", thing));
    }

}
