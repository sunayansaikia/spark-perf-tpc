package org.ssaikia.realm.spark;

import com.databricks.spark.sql.perf.Benchmark;
import com.databricks.spark.sql.perf.Query;
import com.databricks.spark.sql.perf.Variation;
import com.databricks.spark.sql.perf.tpcds.TPCDS;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RunTpcds {
    public static void main(String[] args) {

        SparkSession spark = null;
        try {
            spark = SparkSession
                    .builder()
                    .appName("Spark Hive Generate TPCDS data")
                    .enableHiveSupport()
                    .getOrCreate();

            try {
                String logLevel = spark.conf().get("spark.sql.job.log.level");
                if (logLevel != null) {
                    System.out.println("------ Setting log level to " + logLevel);
                    spark.sparkContext().setLogLevel(logLevel);
                }
            } catch (Exception e) {
                System.out.println("Default LOG level enabled");
            }

            // Note: Declare "sqlContext" for Spark 2.x version
            SparkContext sparkContext = spark.sparkContext();
            // Note: Declare "sqlContext" for Spark 2.x version
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);


            TPCDS tpcds = new TPCDS(sqlContext);
// Set:
            String databaseName = "tpcds" // name of database with TPCDS data.
            spark.sql("use " + databaseName);
            String resultLocation = "s3a://data/spark/results/tpcds_results"; // place to write results
            String iterations = 1 // how many iterations of queries to run.
            Seq<Query> queries = tpcds.tpcds2_4Queries(); // queries to run.

            List<Variation> jVariationList = new ArrayList<Variation>();
            List<String> trueOrFalse = new ArrayList<String>(1);
            trueOrFalse.add("true");
            Seq seqTrue = scala.collection.JavaConversions.collectionAsScalaIterable(trueOrFalse).toSeq();
            Variation variationItem = new Variation("StandardRun", seqTrue);
            variationItem.

            Seq<Variation> variance = new Seq("StandardRun", );
            long timeout = 24*60*60; // timeout, in seconds.
// Run:
            tpcds.ru
            Benchmark.ExperimentStatus experiment = tpcds.runExperiment(
                    queries, false,
                    iterations,
                    Seq(Variation("StandardRun", Seq("true")),
                    resultLocation, new HashMap<String, String>(0),
                    true);
            experiment.waitForFinish(timeout)

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (spark != null) {
                spark.close();
            }
        }
    }
}
