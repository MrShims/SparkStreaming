package org.mrshim.sparkstreamingservice.process;

import com.example.MessageEventAvro;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.mrshim.sparkstreamingservice.spark.KafkaDsStream;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Component
@RequiredArgsConstructor
public class DataProcessing implements ApplicationRunner, Serializable {

    private final KafkaDsStream kafkaDsStream;
    private final JavaStreamingContext javaStreamingContext;


    @Override
    public void run(ApplicationArguments args) throws Exception {
        JavaInputDStream<ConsumerRecord<String, MessageEventAvro>> stream = kafkaDsStream.stream();
        JavaDStream<MessageEventAvro> map = stream.map(ConsumerRecord::value);
        map.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                List<MessageEventAvro> data = new ArrayList<>();
                while (partition.hasNext()) {
                    com.example.MessageEventAvro next = partition.next();
                    data.add(next);

                }
                ObjectMapper mapper = new ObjectMapper();
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                configuration.set("dfs.replication", "1");

                try (FileSystem fileSystem = FileSystem.get(configuration)) {
                    Path filePath = new Path("/bdpp/dataa/" + new Random().nextInt() * 1000);

                    if (fileSystem.exists(filePath)) {
                        fileSystem.delete(filePath, true);
                    }
                    try (FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
                         BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fsDataOutputStream);
                         SequenceWriter sequenceWriter = mapper.writerFor(com.example.MessageEventAvro.class).writeValues(bufferedOutputStream)) {
                        sequenceWriter.writeAll(data);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });















/*        map.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                ObjectMapper mapper=new ObjectMapper();
                try (FileSystem fileSystem = FileSystem.get(configuration)) {
                    Path filePath = new Path("/bdpp/dataa/deb.txt" );

                    if (!fileSystem.exists(filePath.getParent())) {
                        fileSystem.mkdirs(filePath.getParent()); // Создайте родительский каталог
                    }

                    try (FSDataOutputStream fsDataOutputStream = fileSystem.append(filePath);
                         BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fsDataOutputStream);
                         SequenceWriter sequenceWriter = mapper.writerFor(com.example.MessageEventAvro.class).writeValues(bufferedOutputStream)) {
                        while (partition.hasNext()) {
                            com.example.MessageEventAvro next = partition.next();
                            sequenceWriter.write(next);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });*/
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }
}
