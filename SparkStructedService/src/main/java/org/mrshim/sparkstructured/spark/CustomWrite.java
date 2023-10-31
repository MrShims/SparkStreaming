package org.mrshim.sparkstructured.spark;

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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.*;

public class CustomWrite {
    public static void main(String[] args) throws RestClientException, IOException, TimeoutException, StreamingQueryException {
        SparkConf sparkConf = new SparkConf().setAppName("StructuredKafka").setMaster("local[1]");
        sparkConf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        sparkConf.set("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        //  sparkConf.set("spark.sql.streaming.schemaInference", "true");
        sparkConf.set("spark.sql.avro.schemaProviderClass", "org.apache.spark.sql.avro.SchemaConfluentAvroProvider");

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8085/", 10);

        SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("topic-value");
        String avroSchema = schemaMetadata.getSchema();
        //Посмотреть в динамике

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();



        Dataset<Row> rowDataset = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic")
                .load()
                .withColumn("value", from_avro(expr("substring(value, 6)"), avroSchema));


                //.withColumn("value", from_avro(col("value"),avroSchema));

        rowDataset
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", "file:///D:/status/checkpoints")
                .option("path", "file:///D:/status/data")
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .start()
                .awaitTermination();
    }
}
