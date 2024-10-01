package com.martinywwan;


import com.martinywwan.service.ImdbDataAnalyticsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

/**
 * Application to read IMDB data
 * The aim of the application is to:
 * 1. Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
 * (numVotes/averageNumberOfVotes) * averageRating
 * 2. For these 10 movies, list the persons who are most often credited and list the
 * different titles of the 10 movies.
 */
@ComponentScan
public class Application {

    @Autowired
    private ImdbDataAnalyticsService dataProcessingService;

    public static void main(String[] args) {
        ApplicationContext ac = new AnnotationConfigApplicationContext(Application.class);
        var app = ac.getBean(Application.class);
        app.start();
    }

    public void start() {
        dataProcessingService.run();
    }
}
