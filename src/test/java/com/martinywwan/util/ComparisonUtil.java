package com.martinywwan.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ComparisonUtil {

    public static void compareDatasets(Dataset<Row> ds1,
                                       Dataset<Row> ds2,
                                       int expectedNoOfUniqueRecordsInDs1,
                                       int expectedNoOfUniqueRecordsInDs2,
                                       int expectedNoOfCommonRecords) {
        Dataset<Row> onlyInDs1 = ds1.except(ds2);
        System.out.println("Rows only in Dataset 1");
        onlyInDs1.show(1000, false);

        Dataset<Row> onlyInDs2 = ds2.except(ds1);
        System.out.println("Rows only in Dataset 2");
        onlyInDs2.show(1000, false);

        Dataset<Row> commonRecords = ds1.intersect(ds2);
        System.out.println("Rows common in both Datasets");
        commonRecords.show(1000, false);

        assertEquals(expectedNoOfUniqueRecordsInDs1, onlyInDs1.count());
        assertEquals(expectedNoOfUniqueRecordsInDs2, onlyInDs2.count());
        assertEquals(expectedNoOfCommonRecords, commonRecords.count());
    }
}
