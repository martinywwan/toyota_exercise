package com.martinywwan.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DataProcessingService {

    @Autowired
    private SparkSession sparkSession;

    public void run(){
        Dataset<Row> nameBasicsDs = sparkSession.read().option("delimiter", "\t").format("csv").option("header","true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\name.basics.tsv");
        Dataset<Row> titleBasicsDs = sparkSession.read().option("delimiter", "\t").format("csv").option("header","true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\title.basics.tsv");
        Dataset<Row> titleRatingsDs = sparkSession.read().option("delimiter", "\t").format("csv").option("header","true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\title.ratings.tsv");
//        nameBasicsDs.show();
//        titleBasicsDs.show();
//        titleRatingsDs.show();

        Dataset<Row> titleWithRatingDs = titleBasicsDs.join(titleRatingsDs, titleBasicsDs.col("tconst").equalTo(titleRatingsDs.col("tconst")), "inner");
        titleWithRatingDs.show();
//
//        JavaRDD<String> distFile = sc.textFile("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\name.basics.tsv");
//        JavaRDD<NameBasic> nameBasicsRdd = distFile.map(ImdbMapper.nameBasicMapper);


//        nameBasicsRdd.collect().forEach(System.out::println);
    }
}
