package com.martinywwan.spark.filter;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static com.martinywwan.spark.schema.ImdbSchema.getTitlePrincipalSchema;
import static com.martinywwan.util.ComparisonUtil.compareDatasets;

@ExtendWith(MockitoExtension.class)
public class TestImdbFilter {
    private static SparkSession sparkSession;

    private static JavaSparkContext javaSparkContext;

    @BeforeAll
    public static void init(){
        sparkSession = SparkSession.builder()
                .appName("SparkAppTest")
                .master("local[*]") // Use local mode for testing
                .getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
    }

    @AfterAll
    public static void tearDown() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }

    @Test
    public void testFilterTitlePrincipalsByTitleIds(){
        List<Row> inputPrincipalList = Arrays.asList(
                RowFactory.create("tt0000001", 1, "nm1111111", "category1", "\\N", "\\N"),
                RowFactory.create("tt0000002", 1, "nm2222222", "category2", "\\N", "\\N"),
                RowFactory.create("tt0000002", 2, "nm3333333", "category3", "\\N", "\\N"),
                RowFactory.create("tt0000003", 1, "nm1111111", "category1", "\\N", "\\N"),
                RowFactory.create("tt0000003", 2, "nm4444444", "category4", "\\N", "\\N")
        );
        Object[] inputTitleIdsArr = new Object[]{"tt0000001", "tt0000003", "tt0000004"};
        Dataset<Row> inputPrincipalDs = sparkSession.createDataFrame(inputPrincipalList, getTitlePrincipalSchema());

        List<Row> expectedResultList = Arrays.asList(
                RowFactory.create("tt0000001", 1, "nm1111111", "category1", "\\N", "\\N"),
                RowFactory.create("tt0000003", 1, "nm1111111", "category1", "\\N", "\\N"),
                RowFactory.create("tt0000003", 2, "nm4444444", "category4", "\\N", "\\N")
        );
        Dataset<Row> expectedResultDs = sparkSession.createDataFrame(expectedResultList, getTitlePrincipalSchema());

        Dataset<Row> resultDs = ImdbFilter.filterTitlePrincipalsByTitleIds(inputPrincipalDs, inputTitleIdsArr);
        compareDatasets(expectedResultDs, resultDs, 0, 0, 3);
    }
}
