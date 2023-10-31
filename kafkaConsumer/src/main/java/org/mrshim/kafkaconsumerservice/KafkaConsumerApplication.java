package org.mrshim.kafkaconsumerservice;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.*;

public class KafkaConsumerApplication {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("producer").setMaster("local[*]");

        try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
            kafkaParams.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8085");
            kafkaParams.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

            Set<String> topic = Collections.singleton("topic");
            JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, new Duration(2000));
            JavaInputDStream<ConsumerRecord<String, com.example.MessageEventAvro>> directStream = KafkaUtils.createDirectStream(
                    javaStreamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topic, kafkaParams)
            );

            JavaDStream< com.example.MessageEventAvro> data = directStream.map(ConsumerRecord::value);


            Configuration configuration=new Configuration();
            configuration.set("fs.file.impl", LocalFileSystem.class.getName());

            FileSystem fileSystem=FileSystem.get(configuration);


            Path outPath = new Path("/hadoop/dfs/name/mydata.text");


             try (FSDataOutputStream outputStream = fileSystem.create(outPath)) {
                BufferedOutputStream bufferedOutputStream=new BufferedOutputStream(outputStream);
                data.foreachRDD(rdd -> {
                    rdd.foreachPartition(partition -> {
                        while (partition.hasNext()) {
                            com.example.MessageEventAvro next = partition.next();
                            bufferedOutputStream.write(next.getMessage().toString().getBytes());
                            System.out.println("СОООБЩЕНИЕ ПРИШЛО ОТ " + next.getMessage());
                        }
                    });
                });
            } catch (IOException e) {
                 throw new RuntimeException(e);
             }






/*            org.apache.hadoop.haddopConf.Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://namenode:9000");
            JavaDStream< com.example.MessageEventAvro> data = directStream.map(ConsumerRecord::value);
            Configuration hadoopConf = new Configuration();
            FileSystem fs = FileSystem.get(hadoopConf);
            Path outPath = new Path("/hadoop/dfs/name/mydata.text");

            try (FSDataOutputStream outputStream = fs.create(outPath)) {
                data.foreachRDD(rdd -> {
                    rdd.foreachPartition(partition -> {
                        while (partition.hasNext()) {
                            com.example.MessageEventAvro next = partition.next();
                            outputStream.writeBytes(next.getMessage().toString());
                            System.out.println("СОООБЩЕНИЕ ПРИШЛО ОТ " + next.getMessage());
                        }
                    });
                });
            }*/
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();

        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }


    }
}
