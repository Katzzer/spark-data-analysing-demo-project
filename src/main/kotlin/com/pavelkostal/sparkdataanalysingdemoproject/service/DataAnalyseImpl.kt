package com.pavelkostal.sparkdataanalysingdemoproject.service

import com.pavelkostal.sparkdataanalysingdemoproject.model.TobaccoUseColumn
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.springframework.boot.autoconfigure.context.LifecycleProperties
import org.springframework.stereotype.Component
import org.springframework.web.multipart.MultipartFile

@Component
class DataAnalyseImpl() : DataAnalyse {
    override fun analyzeDataFromCsv(file: MultipartFile): String {
        // Convert the uploaded CSV file to a list of strings (one string for each line)
        val csvData = file.inputStream.bufferedReader().readLines()

        // Check if CSV data is empty or invalid
        if (csvData.isEmpty()) {
            return "Uploaded CSV is empty."
        }

//        val header = csvData.first() // First row is the header
        val data = csvData.drop(1)   // Exclude the header row

        val spark = getSparkSession()

        // Check if data contains rows
        if (data.isEmpty()) {
            return "No data rows present in the CSV."
        }

        val javaSparkContext = JavaSparkContext(spark.sparkContext())

        // Define schema (assuming YEAR is stored as IntegerType)
        val schema = getSchema()

        // Preprocess rows to handle invalid YEAR values
        val rows = data.map { line ->
            val split = line.split(",").map { it.trim() }
            RowFactory.create(*split.toTypedArray()) // Convert `split` list to varargs for RowFactory
        }

        // Skip if rows are empty after preprocessing
        if (rows.isEmpty()) {
            return "No valid data rows present in the CSV."
        }

        val rowRDD = javaSparkContext.parallelize(rows)

        val dataFrame = spark.createDataFrame(rowRDD, schema)

        val maleCount = dataFrame
            .filter(col(TobaccoUseColumn.GENDER.columnName).equalTo("Male"))
            .count()

        val femaleCount = dataFrame
            .filter(col(TobaccoUseColumn.GENDER.columnName).equalTo("Female"))
            .count()

        val overallCount = dataFrame
            .filter(col(TobaccoUseColumn.GENDER.columnName).equalTo("Overall"))
            .count()

        return "Number of rows: ${rows.size}\nMale count: $maleCount\nFemale count: $femaleCount\nOverall count: $overallCount"

    }

    private fun getSparkSession(): SparkSession {
        return SparkSession.builder()
            .appName("DataAnalyse")
            .master("local[*]") // Use all available cores on the local machine
            .config("spark.driver.host", "127.0.0.1") // Ensure correct driver host
            .getOrCreate()
    }

    private fun getSchema(): StructType {
        val schema = StructType(
            arrayOf(
                DataTypes.createStructField(TobaccoUseColumn.YEAR.columnName, TobaccoUseColumn.YEAR.dataType, false), //1
                DataTypes.createStructField(TobaccoUseColumn.LOCATION_ABBR.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //2
                DataTypes.createStructField(TobaccoUseColumn.LOCATION_DESC.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //3
                DataTypes.createStructField(TobaccoUseColumn.TOPIC_TYPE.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //4
                DataTypes.createStructField(TobaccoUseColumn.TOPIC_DESC.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //5
                DataTypes.createStructField(TobaccoUseColumn.MEASURE_DESC.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //6
                DataTypes.createStructField(TobaccoUseColumn.DATA_SOURCE.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //7
                DataTypes.createStructField(TobaccoUseColumn.RESPONSE.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //8
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_UNIT.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //9
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_TYPE.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //10
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //11
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_FOOTNOTE_SYMBOL.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //12
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_FOOTNOTE.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //13
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_STD_ERR.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //14
                DataTypes.createStructField(TobaccoUseColumn.LOW_CONFIDENCE_LIMIT.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //15
                DataTypes.createStructField(TobaccoUseColumn.HIGH_CONFIDENCE_LIMIT.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //16
                DataTypes.createStructField(TobaccoUseColumn.SAMPLE_SIZE.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //17
                DataTypes.createStructField(TobaccoUseColumn.GENDER.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //18
                DataTypes.createStructField(TobaccoUseColumn.RACE.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //19
                DataTypes.createStructField(TobaccoUseColumn.AGE.columnName, TobaccoUseColumn.AGE.dataType, false), //20
                DataTypes.createStructField(TobaccoUseColumn.EDUCATION.columnName, TobaccoUseColumn.AGE.dataType, false), //21
                DataTypes.createStructField(TobaccoUseColumn.GEOLOCATION.columnName, TobaccoUseColumn.AGE.dataType, false), //22
                DataTypes.createStructField(TobaccoUseColumn.TOPIC_TYPE.columnName, TobaccoUseColumn.AGE.dataType, false), //23
                DataTypes.createStructField(TobaccoUseColumn.TOPIC_ID.columnName, TobaccoUseColumn.AGE.dataType, false), //24
                DataTypes.createStructField(TobaccoUseColumn.MEASURE_ID.columnName, TobaccoUseColumn.AGE.dataType, false), //25
                DataTypes.createStructField(TobaccoUseColumn.STRATIFICATION_ID1.columnName, TobaccoUseColumn.AGE.dataType, false), //26
                DataTypes.createStructField(TobaccoUseColumn.STRATIFICATION_ID2.columnName, TobaccoUseColumn.AGE.dataType, false), //27
                DataTypes.createStructField(TobaccoUseColumn.STRATIFICATION_ID3.columnName, TobaccoUseColumn.AGE.dataType, false), //28
                DataTypes.createStructField(TobaccoUseColumn.STRATIFICATION_ID4.columnName, TobaccoUseColumn.AGE.dataType, false), //29
                DataTypes.createStructField(TobaccoUseColumn.SUB_MEASURE_ID.columnName, TobaccoUseColumn.AGE.dataType, false), //30
                DataTypes.createStructField(TobaccoUseColumn.DISPLAY_ORDER.columnName, TobaccoUseColumn.AGE.dataType, false), //30
            )
        )
        return schema
    }


}