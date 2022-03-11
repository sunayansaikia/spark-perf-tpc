package org.ssaikia.realm.spark;

import com.databricks.spark.sql.perf.tpcds.TPCDSTables;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class GenDataTpcds {

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


            // Set:
            // Note: Here my env is using MapRFS, so I changed it to "hdfs:///tpcds".
            // Note: If you are using HDFS, the format should be like "hdfs://namenode:9000/tpcds"
            String rootDir = "s3a://data/spark/tpcds"; // root directory of location to create data in.

            String databaseName = "tpcds"; // name of database to create.
            String scaleFactor = "1"; // scaleFactor defines the size of the dataset to generate (in GB).
            String format = "parquet"; // valid spark format like parquet "parquet".
            // Run:
            TPCDSTables tables = new TPCDSTables(sqlContext,
                    "/tools/tpcds-kit/tools", // location of tpcds-ket/tools directory where dsdgen executable is in the spark executor nodes
                    "1",
                    false, // true to replace DecimalType with DoubleType
                    false); // true to replace DateType with StringType

            tables.genData(
                    rootDir,
                    format,
                    true, // overwrite the data that is already there
                    true, // create the partitioned fact tables
                    true, // shuffle to get partitions coalesced into single files.
                    false, // true to filter out the partition with NULL key value
                    "customer", // "" means generate all tables
                    2); // how many dsdgen partitions to run - number of input tasks.

            // Create the specified database
            spark.sql("create database if not exists " + databaseName);
            // Create metastore tables in a specified database for your data.
            // Once tables are created, the current database will be switched to the specified database.
            tables.createExternalTables(rootDir, "parquet", databaseName, true, true, "customer");
            // Or, if you want to create temporary tables
            // tables.createTemporaryTables(location, format)

            // For CBO only, gather statistics on all columns:
            tables.analyzeTables(databaseName, true, "customer");


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (spark != null) {
                spark.close();
            }
        }
    }
}
