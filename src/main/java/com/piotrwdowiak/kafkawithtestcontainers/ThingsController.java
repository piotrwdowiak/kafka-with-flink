package com.piotrwdowiak.kafkawithtestcontainers;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ThingsController {

    @Autowired
    private KafkaProducer producer;

    @GetMapping(path = "/produce")
    public void produce() {
        producer.send("things_to_flink", "Sending with own controller");
    }
}
