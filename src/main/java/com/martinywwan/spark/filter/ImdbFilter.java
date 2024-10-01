package com.martinywwan.spark.filter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ImdbFilter {

    public static Dataset<Row> filterTitlePrincipalsByTitleIds(Dataset<Row> titlePrincipalsDs, Object[] titleIdsArr){
        Dataset<Row> titlePrincipalsFilteredDs = titlePrincipalsDs.filter(titlePrincipalsDs.col("tconst").isin(titleIdsArr));
        return titlePrincipalsFilteredDs;
    }

    public static Dataset<Row> filterNamesByGivenList(Dataset<Row> namesDs, Object[] top10TitleCreditedPersonsArr) {
        Dataset<Row> titlePrincipalsFilteredDs = namesDs.filter(namesDs.col("nconst").isin(top10TitleCreditedPersonsArr));
        return titlePrincipalsFilteredDs;
    }
}
