package com.martinywwan.spark.schema;

import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

public class ImdbSchema {

    public static final StructType getTitleRatingSchema(){
        StructType schema = new StructType();
        schema = schema.add("tconst", StringType, false);
        schema = schema.add("averageRating", DoubleType, false);
        schema = schema.add("numVotes", DoubleType, false);
        return schema;
    }

    public static final StructType getTitleBasicSchema(){
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
        return schema;
    }

    public static final StructType getTitlePrincipalSchema(){
        StructType schema = new StructType();
        schema = schema.add("tconst", StringType, false);
        schema = schema.add("ordering", IntegerType, false);
        schema = schema.add("nconst", StringType, false);
        schema = schema.add("category", StringType, false);
        schema = schema.add("job", StringType, false);
        schema = schema.add("characters", StringType, false);
        return schema;
    }

    public static final StructType getNameBasicSchema(){
        StructType schema = new StructType();
        schema = schema.add("nconst", StringType, false);
        schema = schema.add("primaryName", StringType, false);
        schema = schema.add("birthYear", StringType, false);
        schema = schema.add("deathYear", StringType, false);
        schema = schema.add("primaryProfession", StringType, false);
        schema = schema.add("knownForTitles", StringType, false);
        return schema;
    }

    public static final StructType getTitleAkasSchema(){
        StructType schema = new StructType();
        schema = schema.add("titleId", StringType, false);
        schema = schema.add("ordering", IntegerType, false);
        schema = schema.add("title", StringType, false);
        schema = schema.add("region", StringType, false);
        schema = schema.add("language", StringType, false);
        schema = schema.add("types", StringType, false);
        schema = schema.add("attributes", StringType, false);
        schema = schema.add("isOriginalTitle", StringType, false);
        return schema;
    }
}
