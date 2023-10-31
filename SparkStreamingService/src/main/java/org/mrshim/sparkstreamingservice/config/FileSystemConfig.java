package org.mrshim.sparkstreamingservice.config;


import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;


@org.springframework.context.annotation.Configuration
public class FileSystemConfig {

    @Bean
    public Configuration configuration()
    {
        return new Configuration();
    }
    @Bean
    @SneakyThrows
    public FileSystem fileSystem(Configuration configuration)
    {
        return FileSystem.get(configuration);
    }






}
