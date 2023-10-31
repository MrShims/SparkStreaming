package org.mrshim.sparkstructured;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import za.co.absa.abris.config.AbrisConfig;
import za.co.absa.abris.config.FromAvroConfig;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static za.co.absa.abris.avro.functions.from_avro;


public class SparkStructuredServiceApplication {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException, RestClientException, IOException {

        SparkConf sparkConf = new SparkConf().setAppName("StructuredKafka").setMaster("local[1]");
        sparkConf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        sparkConf.set("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        sparkConf.set("spark.sql.avro.schemaProviderClass", "org.apache.spark.sql.avro.SchemaConfluentAvroProvider");

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8085/", 10);

        SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("topic-value");
        String avroSchema = schemaMetadata.getSchema();

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        FromAvroConfig abrisConfig = AbrisConfig
                .fromConfluentAvro()
                .downloadReaderSchemaById(3)
                .usingSchemaRegistry("http://localhost:8085/");



        Dataset<Row> rowDataset = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic")
                .load().select(from_avro(col("value"), abrisConfig).alias("Event"));

        rowDataset
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", "file:///D:/status/checkpoints")
                .option("path", "file:///D:/status/data")
                .option("compression", "gzip")
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .start()
                .awaitTermination();


        // .withColumn("value", functions.expr("cast(value as string)"));;//.withColumn("value", from_avro(col("value"),avroSchema));;
/*
        StructType avroSchema1 = new StructType()
                .add("id", "long")
                .add("message", "string");
*/
        //     String avroSchemaJSON = "{\"type\":\"record\",\"name\":\"MessageEventAvro\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"message\",\"type\":\"string\"}]}";
        //  rowDataset=rowDataset.withColumn("value", from_avro(col("value"),avroSchema));



        /*        Dataset<Row> deserializedData = load.selectExpr("CAST(value AS BINARY) as avroData")
                .select(from_avro(col("avroData"), avroSchema).as("deserializedData"))
                .select("deserializedData.*");*/
        //    load.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
/*        load = load
                .withColumn("value", callUDF("deserializeAvro", col("value")))
                .selectExpr("CAST(value AS STRING");*/
/*
        load.writeStream()
                .format("parquet")
                .option("path", "D:\\writedata")
                .option("checkpointLocation", "D:\\status")
                .start()
                .awaitTermination();*/
    }
}
