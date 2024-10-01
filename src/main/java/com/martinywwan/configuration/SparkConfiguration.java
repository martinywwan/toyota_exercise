package com.martinywwan.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spark Configuration class to create singleton spark instance
 */
@Configuration
public class SparkConfiguration {

    @Bean
    public SparkSession sparkSession() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("IMDB dataset processor")
                .master("local[*]")
                .getOrCreate();
        return sparkSession;
    }
}
