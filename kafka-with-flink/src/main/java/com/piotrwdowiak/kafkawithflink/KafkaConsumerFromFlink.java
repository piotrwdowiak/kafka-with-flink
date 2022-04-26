package com.piotrwdowiak.kafkawithflink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerFromFlink {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerFromFlink.class);

    @KafkaListener(topics = "things_from_flink")
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        LOGGER.info("received payload from Flink='{}'", consumerRecord.toString());
    }


}
