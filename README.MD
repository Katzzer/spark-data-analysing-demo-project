# Apache Spark Demo project

## Description
- Data analysis with Apache Spark
- Data are from [CDC Behavioral Risk Factor Data: Tobacco Use (2011 to present)](https://data.cdc.gov/Survey-Data/Behavioral-Risk-Factor-Data-Tobacco-Use-2011-to-pr/wsas-xwh5/about_data)

## Basic information
- For better compatibility is used Spring Boot "2.7.5" (with version 3+ there is a lot of problems, for example logging, data processing etc.)
- Spark is programmed in Scala, so not all works fine with Spring Boot 3+ version (I was not able to  run application and process data)

### How to analyse data with Apache Spark

1. Start this application
2. Open Postman and prepare POST request to `localhost:8080/api/v1/analysis/analyze-data`
    - Go to `Body` select `form-data`, in `key` set name to `file` and type select `file` and in `Value` select file `csvData.csv` and add file
      that is in this project in `/data` directory
3. Send request
4. Optional: Run `docker-compose up` to run master and worked node for Apache Spark with Hadoop
5. Set configuration
```kotlin
val spark = SparkSession.builder()
            .appName("Kotlin Spark Application")
            .master("spark://localhost:7077")  // Connect to the master in Docker
            .config("spark.driver.host", "127.0.0.1") // Optional: driver host must be accessible
            .getOrCreate()
```

### Some more information
- It will start `Apache Spark` on this local PC
- It is possible to configure Apache Spark locally, to use Master and Worker Node and also use Hadoop for data
```kotlin
        val spark = SparkSession.builder() // only Spark
            .appName("Kotlin Spark Application")
            .master("spark://localhost:7077")  // Connect to the master in Docker
            .config("spark.driver.host", "127.0.0.1") // Optional: driver host must be accessible
            .getOrCreate()
```
- You could also use Hadoop for data storage:
```kotlin
        val spark = SparkSession.builder() // Apache Spark with Hadoop
            .appName("Kotlin Spark Application")
            .master("spark://localhost:7077")  // Spark Master in Docker
            .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-master:9000") // Namenode address
            .config("spark.hadoop.dfs.replication", "1") // Optional: replication factor
            .getOrCreate()
```
- Apache Spark Web UI (if started from Docker) runs on [http://localhost:8090/](http://localhost:8090/) there you can see information about running tasks
[//]: # (- It is possible to warm-up Spark when application start, just use @Bean)
