# spark-perf-

#Command to install jar locally
mvn install:install-file -Dfile='lib/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar' -DgroupId=databricks -DartifactId=spark-sql-perf -Dversion='1.0.0' -Dpackaging=jar -DlocalRepositoryPath=lib -DcreateChecksum=true