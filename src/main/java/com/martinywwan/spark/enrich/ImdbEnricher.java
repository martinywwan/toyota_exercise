package com.martinywwan.spark.enrich;

import com.martinywwan.configuration.AppProperties;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ImdbEnricher {

    public Dataset<Row> enrichTitlesWithRatings(Dataset<Row> titleBasicsDs, Dataset<Row> titleRatingsDs){
        var titleRatingsRenamed = titleRatingsDs.withColumnRenamed("tconst", "titleRatingTconst");
        Column joinCondition = titleBasicsDs.col("tconst").equalTo(titleRatingsRenamed.col("titleRatingTconst"));
        var datasetJoined = titleBasicsDs.join(titleRatingsRenamed, joinCondition, "inner");
        return datasetJoined.drop("titleRatingTconst");
    }

    public Dataset<Row> enrichPrincipalsWithNames(Dataset<Row> titlePrincipalsDs, Dataset<Row> namesDs){
        var namesRenamedDs = namesDs.withColumnRenamed("nconst", "namesNconst");
        Column joinCondition = namesRenamedDs.col("namesNconst").equalTo(titlePrincipalsDs.col("nconst"));
        var datasetJoined = titlePrincipalsDs.join(namesRenamedDs, joinCondition, "left");
        return datasetJoined.drop("namesNconst");
    }
}
