package com.martinywwan.service;

import com.martinywwan.spark.repository.ImdbRepository;
import com.martinywwan.spark.enrich.ImdbEnricher;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.reflect.ClassTag;

import static com.martinywwan.spark.filter.ImdbFilter.filterNamesByGivenList;
import static com.martinywwan.spark.filter.ImdbFilter.filterTitlePrincipalsByTitleIds;
import static org.apache.spark.sql.functions.*;

@Service
public class ImdbDataAnalyticsService {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private ImdbEnricher imdbEnricher;

    @Autowired
    private ImdbRepository imdbRepository;

    /** Retrieve the top 10 movies that have a minimum of 500 votes.
     *
     * @param titleBasicsDs dataset containing movie titles
     * @param titleRatingsDs dataset containing ratings of movies
     * @return top 10 rated movies
     */
    public Dataset<Row> getTop10Titles(Dataset<Row> titleWithRatingsDs) {
        Dataset<Row> titleWithRatingDsCached = titleWithRatingsDs.cache();

        // Broadcast the variable avgNumOfVotes to all workers
        Double avgNumOfVotes = titleWithRatingDsCached.groupBy().mean("numVotes").as("avgNumOfVotes").first().getDouble(0);
        ClassTag<Double> classTagTest = scala.reflect.ClassTag$.MODULE$.apply(Double.class);
        Broadcast<Double> avgNumOfVotesBroadcast = sparkSession.sparkContext().broadcast(avgNumOfVotes, classTagTest);

        Column overallRatingDefinition = (titleWithRatingDsCached.col("numVotes").divide(avgNumOfVotesBroadcast.getValue()))
                .multiply(titleWithRatingDsCached.col("averageRating"));
        Dataset<Row> titleWithOverallRatings = titleWithRatingDsCached.filter(titleWithRatingDsCached.col("numVotes").$greater$eq(500))
                .withColumn("overallRating", overallRatingDefinition);
        avgNumOfVotesBroadcast.unpersist();

        return titleWithOverallRatings.orderBy(desc("overallRating")).limit(10);
    }

    public Dataset<Row> getMostCreditedPersonByTitle(Dataset<Row> titlesWithCreditedNames){
        Dataset<Row> mostCreditPersonsByTitle = titlesWithCreditedNames
                // Group by tconst (title) and primaryName (person)
                .groupBy("tconst", "primaryName")
                // Count the occurrences of each primaryName (person) for each title
                .agg(count("primaryName").alias("credits_count"))
                // Use Window function to rank persons within each title based on their credits count
                .withColumn("rank", row_number().over(
                        Window.partitionBy("tconst").orderBy(desc("credits_count"))
                ))
                // Filter only the top person (rank = 1) for each title
                .filter(col("rank").equalTo(1))
                .select("tconst", "primaryName", "credits_count");
        return mostCreditPersonsByTitle;
    }

    public Dataset<Row> getAlternativeTitles(Dataset<Row> titlesDs, Dataset<Row> alternativeTitles){
        Dataset<Row> titlesReducedColsDs = titlesDs.drop("titleType", "isAdult", "startYear", "endYear",
                "runtimeMinutes", "genres", "averageRating", "numVotes", "overallRating", "originalTitle");
        Dataset<Row> alternativeTitlesReducedColsDs = alternativeTitles.drop("ordering", "types", "attributes");
        Column joinCondition = titlesReducedColsDs.col("tconst").equalTo(alternativeTitlesReducedColsDs.col("titleId"));
        Dataset<Row> alternativeTitlesDs = alternativeTitlesReducedColsDs.join(broadcast(titlesReducedColsDs), joinCondition, "right");
        return alternativeTitlesDs.withColumnRenamed("title", "alternativeTitle").drop("titleId");
    }

    public void run() {
        // First retrieve all the required datasets
        Dataset<Row> titleRatingsDs = imdbRepository.getTitleRatingsDataset();
        Dataset<Row> titleBasicsDs = imdbRepository.getTitleBasicsDataset();
        Dataset<Row> titlePrincipalsDs = imdbRepository.getTitlePrincipalsDataset();
        Dataset<Row> namesDs = imdbRepository.getNamesDataset();
        Dataset<Row> titleAkasDs = imdbRepository.getTitleAkas();

        // Calculate the top 10 titles
        Dataset<Row> titleWithRatingsDs = imdbEnricher.enrichTitlesWithRatings(titleBasicsDs, titleRatingsDs);
        Dataset<Row> top10TitlesDs = getTop10Titles(titleWithRatingsDs);
        System.out.println("Top 10 Titles");
        top10TitlesDs.show();

        // Convert top 10 title id's to an array
        Object[] top10TitlesArr = top10TitlesDs.select("tconst")
                .as(Encoders.STRING()) // Convert to a Dataset<String>
                .collectAsList().toArray();

        // filter title principles dataset to only include top 10 titles
        Dataset<Row> titlePrincipalsFilteredDs = filterTitlePrincipalsByTitleIds(titlePrincipalsDs, top10TitlesArr);
        Dataset<Row> titlePrincipalsFilteredDsCached = titlePrincipalsFilteredDs.cache();

        // Get distinct list of names using the top 10 titles
        Object[] top10TitleCreditedPersonsArr = titlePrincipalsFilteredDsCached.select("nconst").distinct()
                .as(Encoders.STRING()) // Convert to a Dataset<String>
                .collectAsList().toArray();

        // Filter names by those that we are interested in
        Dataset<Row> namesFilteredDs = filterNamesByGivenList(namesDs, top10TitleCreditedPersonsArr);

        // Enrich title principals with full names
        Dataset<Row> principalsWithNames = imdbEnricher.enrichPrincipalsWithNames(titlePrincipalsFilteredDsCached, namesFilteredDs);

        System.out.println("Most credited person by title");
        Dataset<Row> mostCreditedPersonsByTitle = getMostCreditedPersonByTitle(principalsWithNames);
        mostCreditedPersonsByTitle.show();
        // Limitation: The function above does not show all titles if the title did not exist within the principles

        Dataset<Row> alternativeTitlesDs = getAlternativeTitles(top10TitlesDs, titleAkasDs);
        alternativeTitlesDs.show();

    }

}