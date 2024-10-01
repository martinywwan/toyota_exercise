package com.martinywwan.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Getter
@Setter
@Configuration
@PropertySource("classpath:application-${spring.profiles.active}.properties")
public class AppProperties {

    @Value("${datasource.title.akas.path}")
    private String datasourceTitleAkasPath;

    @Value("${datasource.name.basics.path}")
    private String datasourceNameBasicsPath;

    @Value("${datasource.title.principals.path}")
    private String datasourceTitlePrincipalsPath;

    @Value("${datasource.title.basics.path}")
    private String datasourceTitleBasicsPath;

    @Value("${datasource.title.ratings.path}")
    private String datasourceTitleRatingsPath;
}