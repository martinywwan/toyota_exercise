package com.martinywwan.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Spark Configuration class to create singleton spark instance
 */
@Configuration
@PropertySource("classpath:application-${spring.profiles.active}.properties")
public class SparkConfiguration {

    @Value("${app.name}")
    private String applicationName;

    @Bean
    public SparkSession sparkSession() {
        SparkSession sparkSession = SparkSession.builder()
                .appName(applicationName)
                .master("local[*]")
                .getOrCreate();
        return sparkSession;
    }
}
