package com.martinywwan.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestDataProcessingService {

    @Test
    public void testExample() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("IMDB dataset processor")
                .master("local[*]")
                .getOrCreate();
        List<Double> points = Arrays.asList(1.3, 2.2, 3.3, 4.4, 5.0);
        var dataset = sparkSession.createDataset(points, Encoders.DOUBLE()).toDF("values");
        dataset.show();
        var s = dataset.groupBy().mean("values").as("avgNumOfVotes").first().getDouble(0);
        System.out.println(s);
    }


    @Test
    public void test2(){
        SparkSession sparkSession = SparkSession.builder()
                .appName("IMDB dataset processor")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> titleBasicsDs = sparkSession.read().option("delimiter", "\t").format("csv").option("header", "true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\title.basics.tsv");
        Dataset<Row> titleRatingsDs = sparkSession.read().option("delimiter", "\t").format("csv").option("header", "true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\title.ratings.tsv");

    }
}
