package com.martinywwan.spark.repository;

import com.martinywwan.configuration.AppProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import static com.martinywwan.spark.schema.ImdbSchema.*;

@Repository
public class ImdbRepository {

    @Autowired
    private AppProperties appProperties;

    @Autowired
    private SparkSession sparkSession;

    public Dataset<Row> getTitleRatingsDataset() {
        var schema = getTitleRatingSchema();
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load(appProperties.getDatasourceTitleRatingsPath());
    }

    public Dataset<Row> getTitleBasicsDataset() {
        var schema = getTitleBasicSchema();
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load(appProperties.getDatasourceTitleBasicsPath());
    }

    public Dataset<Row> getTitlePrincipalsDataset() {
        var schema = getTitlePrincipalSchema();
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load(appProperties.getDatasourceTitlePrincipalsPath());
    }

    public Dataset<Row> getNamesDataset() {
        var schema = getNameBasicSchema();
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load(appProperties.getDatasourceNameBasicsPath());
    }


    public Dataset<Row> getTitleAkas() {
        var schema = getTitleAkasSchema();
        return sparkSession.read().schema(schema).option("delimiter", "\t").format("csv").option("header", "true").load(appProperties.getDatasourceTitleAkasPath());
    }

}
