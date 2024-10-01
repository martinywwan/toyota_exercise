package com.martinywwan.spark.repository;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import static org.apache.spark.sql.types.DataTypes.*;

@Repository
public class ImdbRepository {

    @Autowired
    private SparkSession sparkSession;

    public Dataset<Row> getTitleRatingsDataset() {
        StructType schema = new StructType();
        schema = schema.add("tconst", StringType, false);
        schema = schema.add("averageRating", DoubleType, false);
        schema = schema.add("numVotes", IntegerType, false);
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\title.ratings.tsv");
    }

    public Dataset<Row> getTitleBasicsDataset() {
        StructType schema = new StructType();
        schema = schema.add("tconst", StringType, false);
        schema = schema.add("titleType", StringType, false);
        schema = schema.add("primaryTitle", StringType, false);
        schema = schema.add("originalTitle", StringType, false);
        schema = schema.add("isAdult", IntegerType, false);
        schema = schema.add("startYear", StringType, false);
        schema = schema.add("endYear", StringType, false);
        schema = schema.add("runtimeMinutes", IntegerType, false);
        schema = schema.add("genres", StringType, false);
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\title.basics.tsv");
    }

    public Dataset<Row> getTitlePrincipalsDataset() {
        StructType schema = new StructType();
        schema = schema.add("tconst", StringType, false);
        schema = schema.add("ordering", IntegerType, false);
        schema = schema.add("nconst", StringType, false);
        schema = schema.add("category", StringType, false);
        schema = schema.add("job", StringType, false);
        schema = schema.add("characters", StringType, false);
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files_subset\\title.principals.tsv");
    }

    public Dataset<Row> getNamesDataset() {
        StructType schema = new StructType();
        schema = schema.add("nconst", StringType, false);
        schema = schema.add("primaryName", StringType, false);
        schema = schema.add("birthYear", StringType, false);
        schema = schema.add("deathYear", StringType, false);
        schema = schema.add("primaryProfession", StringType, false);
        schema = schema.add("knownForTitles", StringType, false);

        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files\\name.basics.tsv");
    }


    public Dataset<Row> getTitleAkas() {
        StructType schema = new StructType();
        schema = schema.add("titleId", StringType, false);
        schema = schema.add("ordering", IntegerType, false);
        schema = schema.add("title", StringType, false);
        schema = schema.add("region", StringType, false);
        schema = schema.add("language", StringType, false);
        schema = schema.add("types", StringType, false);
        schema = schema.add("attributes", StringType, false);
        schema = schema.add("isOriginalTitle", StringType, false);
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load("C:\\Users\\MW\\OneDrive\\Desktop\\files\\data_files\\title.akas.tsv");
    }

}
