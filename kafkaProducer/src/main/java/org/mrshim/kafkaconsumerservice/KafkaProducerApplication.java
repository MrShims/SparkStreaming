package org.mrshim.kafkaconsumerservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaProducerApplication
{
    //filehost и hostnane file corsite и hdfs site
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApplication.class,args);
    }
}
