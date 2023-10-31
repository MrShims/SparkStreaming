package org.mrshim.kafkaconsumerservice.producer;


import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
public class KafkaProducer {

    @Value("${kafka.topic}")
    private String topicName;

    private final KafkaTemplate<String, com.example.MessageEventAvro> kafkaTemplate;

    AtomicInteger atomicInteger=new AtomicInteger(0);


    @Scheduled(fixedDelay = 20)
    public void sendMessage()
    {

        com.example.MessageEventAvro messageEventAvro=new com.example.MessageEventAvro();
        messageEventAvro.setId(String.valueOf(atomicInteger.getAndIncrement()));
        messageEventAvro.setMessage("Message getData "+atomicInteger.get());
        ProducerRecord<String, com.example.MessageEventAvro> message=new ProducerRecord<>(topicName,String.valueOf(messageEventAvro.getId()),messageEventAvro);
        kafkaTemplate.send(message);
    }
}
