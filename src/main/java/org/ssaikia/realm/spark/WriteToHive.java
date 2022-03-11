package org.ssaikia.realm.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;


/**
 *
 */
public class WriteToHive {

    public static void main(String[] args) {
        String tableName = "customer";

        if(args.length > 0){
            tableName = args[0];
        }
        SparkSession spark = null;
        try{
            spark = SparkSession
                    .builder()
                    .appName("Spark Hive Integration Test")
                    .enableHiveSupport()
                    .getOrCreate();
            try{
                String logLevel = spark.conf().get("spark.sql.job.log.level");
                if(logLevel!=null){
                    System.out.println("------ Setting log level to "+logLevel);
                    spark.sparkContext().setLogLevel(logLevel);
                }
            } catch (Exception e){
                System.out.println("Default LOG level enabled");
            }

            Dataset<Row> testData = spark.createDataFrame(Arrays.asList(
                    new Customer("John Doe", "Singapore"),
                    new Customer("Jane Doe", "India"),
                    new Customer("John Smith", "India"),
                    new Customer("Jane Smith", "Singapore")
                    ), Customer.class);
            testData.show(false);
            testData.write()
                    .option("path", "s3a://data/spark/tables/" + tableName)
                    .mode("overwrite")
                    .saveAsTable("default." + tableName);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if(spark!=null){
                spark.close();
            }
        }
    }
}
