package org.mrshim.sparkstreamingservice.config;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class StreamConfig {

    private final JavaSparkContext javaSparkContext;

    @Bean
    public JavaStreamingContext streamingContext()
    {

        return new JavaStreamingContext(javaSparkContext,new Duration(20000));

    }
}
