package org.mrshim.sparkstructured.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
/*
@Component
public class SparkStream implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {

        SparkConf sparkConf=new SparkConf().setAppName("StructedKafka").setMaster("local[1]");

        SparkSession sparkSession= SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();


        Dataset<Row> load = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "http://localhost:9092")
                .option("subscribe", "topic")
                .load();

        load.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

*//*        load = load
                .withColumn("value", callUDF("deserializeAvro", col("value")))
                .selectExpr("CAST(value AS STRING");*//*

        load.writeStream()
                .format("parquet")
                .option("path", "D:\\writedata")
                .option("checkpointLocation", "D:\\status")
                .start()
                .awaitTermination();


    }
}*/
