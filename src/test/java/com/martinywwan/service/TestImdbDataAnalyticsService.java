package com.martinywwan.service;

import com.martinywwan.spark.enrich.ImdbEnricher;
import com.martinywwan.spark.repository.ImdbRepository;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.martinywwan.util.ComparisonUtil.compareDatasets;
import static org.apache.spark.sql.types.DataTypes.*;

import java.util.Arrays;
import java.util.List;


@ExtendWith(MockitoExtension.class)
public class TestImdbDataAnalyticsService {

    @Spy
    private SparkSession sparkSession = SparkSession.builder()
            .appName("SparkAppTest")
            .master("local[*]")
            .getOrCreate();

    @Mock
    private ImdbEnricher imdbEnricher;

    @Mock
    private ImdbRepository imdbRepository;

    @InjectMocks
    private ImdbDataAnalyticsService imdbDataAnalyticsService;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void tearDown() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }

    @Test
    public void testGetTop10Titles() {
        List<Row> inputTitleWithRatingsList = Arrays.asList(
                RowFactory.create("tt0000001", 5.0, 600),
                RowFactory.create("tt0000002", 4.0, 500),
                RowFactory.create("tt0000003", 4.0, 400), // ignored since less than 500 votes
                RowFactory.create("tt0000004", 9.0, 1000),
                RowFactory.create("tt0000005", 9.5, 505)
        ); //601 is the average number of votes
        StructType inputSchema = new StructType();
        inputSchema = inputSchema.add("tconst", StringType, false);
        inputSchema = inputSchema.add("averageRating", DoubleType, false);
        inputSchema = inputSchema.add("numVotes", IntegerType, false);

        Dataset<Row> inputTitleWithRatingsDs = sparkSession.createDataFrame(inputTitleWithRatingsList, inputSchema);

        List<Row> expectedResult = Arrays.asList(
                RowFactory.create("tt0000004", 9.0, 1000, 14.975041597337771),
                RowFactory.create("tt0000005", 9.5, 505, 7.98252911813644),
                RowFactory.create("tt0000001", 5.0, 600, 4.9916805324459235),
                RowFactory.create("tt0000002", 4.0, 500, 3.327787021630616)
        );
        StructType expectedSchema = new StructType();
        expectedSchema = expectedSchema.add("tconst", StringType, false);
        expectedSchema = expectedSchema.add("averageRating", DoubleType, false);
        expectedSchema = expectedSchema.add("numVotes", IntegerType, false);
        expectedSchema = expectedSchema.add("overallRating", DoubleType, false);
        Dataset<Row> expectedDs = sparkSession.createDataFrame(expectedResult, expectedSchema);

        Dataset<Row> resultDs = imdbDataAnalyticsService.getTop10Titles(inputTitleWithRatingsDs);
        compareDatasets(expectedDs, resultDs, 0, 0, 4);
    }
}
