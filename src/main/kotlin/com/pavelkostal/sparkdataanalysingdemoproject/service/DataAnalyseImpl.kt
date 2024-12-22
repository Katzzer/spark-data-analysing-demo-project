package com.pavelkostal.sparkdataanalysingdemoproject.service

import com.pavelkostal.sparkdataanalysingdemoproject.model.TobaccoUseColumn
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.springframework.stereotype.Component
import org.springframework.web.multipart.MultipartFile
import java.io.ByteArrayInputStream
import java.io.File
import java.nio.file.Files

@Component
class DataAnalyseImpl() : DataAnalyse {
    override fun analyzeDataFromCsvFromInternalFile(): String {
        val filePath = File("Data/Behavioral_Risk_Factor_Data__Tobacco_Use__2011_to_present__20241221.csv")

        val file = Files.readAllBytes(filePath.toPath())

        return analyzeDataFromCsv(file)
    }

    override fun analyzeDataFromCsvFromExternalFile(file: MultipartFile):String {
        return analyzeDataFromCsv(file.bytes)
    }

    private fun analyzeDataFromCsv(file: ByteArray): String {
        // Convert the uploaded CSV file to a list of strings (one string for each line)
        val csvData = ByteArrayInputStream(file).bufferedReader().readLines()

        // Check if CSV data is empty or invalid
        if (csvData.isEmpty()) {
            return "Uploaded CSV is empty."
        }

        val data = csvData.drop(1)   // Exclude the header row
        val spark = getSparkSession()
        if (data.isEmpty()) {
            return "No data rows present in the CSV."
        }

        val javaSparkContext = JavaSparkContext(spark.sparkContext())
        val schema = getSchema()

        // Preprocess rows
        val rows = data.map { line ->
            val split = line.split(",").map { it.trim() }
            RowFactory.create(*split.toTypedArray()) // Convert `split` list to varargs for RowFactory
        }

        if (rows.isEmpty()) {
            return "No valid data rows present in the CSV."
        }

        val rowRDD = javaSparkContext.parallelize(rows)
        val dataFrame = spark.createDataFrame(rowRDD, schema)
        val cleanedDataFrame = cleanDataFrame(dataFrame)

        val maleGenderCount = getCountByGender(dataFrame,"Male")
        val femaleGenderCount = getCountByGender(dataFrame,"Female")
        val overallGenderCount = getCountByGender(dataFrame,"Overall")

        val sortedByStatePercentageUsage = getPercentageUsageSortedByState(cleanedDataFrame)
        val topTenByStatesPercentageUsage = getTopTen(sortedByStatePercentageUsage)

        val sortedByAgePercentageUsage = getPercentageUsageSortedByAge(cleanedDataFrame)
        val topTenByAgePercentageUsage = getTopTen(sortedByAgePercentageUsage)

        return returnResultFormatted(rows, maleGenderCount, femaleGenderCount, overallGenderCount, topTenByStatesPercentageUsage, topTenByAgePercentageUsage)
    }

    private fun getCountByGender(filteredDataFrame: Dataset<Row>, gender:String): Long {
        val maleGenderCount = filteredDataFrame
            .filter(col(TobaccoUseColumn.GENDER.columnName).equalTo(gender))
            .count()
        return maleGenderCount
    }

    private fun getSparkSession(): SparkSession {
        return SparkSession.builder()
            .appName("DataAnalyse")
            .master("local[*]") // Use all available cores on the local machine
            .config("spark.driver.memory", "300m")
            .config("spark.executor.memory", "300m")
            .config("spark.executor.instances", "1")
            .config("spark.driver.host", "127.0.0.1") // Ensure correct driver host
            .getOrCreate()
    }

    private fun getSchema(): StructType {
        return StructType(
            arrayOf(
                DataTypes.createStructField(TobaccoUseColumn.YEAR.columnName, TobaccoUseColumn.YEAR.dataType, false), //1
                DataTypes.createStructField(TobaccoUseColumn.LOCATION_ABBR.columnName, TobaccoUseColumn.LOCATION_ABBR.dataType, false), //2
                DataTypes.createStructField(TobaccoUseColumn.LOCATION_DESC.columnName, TobaccoUseColumn.LOCATION_DESC.dataType, false), //3
                DataTypes.createStructField(TobaccoUseColumn.TOPIC_TYPE.columnName, TobaccoUseColumn.TOPIC_TYPE.dataType, false), //4
                DataTypes.createStructField(TobaccoUseColumn.TOPIC_DESC.columnName, TobaccoUseColumn.TOPIC_DESC.dataType, false), //5
                DataTypes.createStructField(TobaccoUseColumn.MEASURE_DESC.columnName, TobaccoUseColumn.MEASURE_DESC.dataType, false), //6
                DataTypes.createStructField(TobaccoUseColumn.DATA_SOURCE.columnName, TobaccoUseColumn.DATA_SOURCE.dataType, false), //7
                DataTypes.createStructField(TobaccoUseColumn.RESPONSE.columnName, TobaccoUseColumn.RESPONSE.dataType, false), //8
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_UNIT.columnName, TobaccoUseColumn.DATA_VALUE_UNIT.dataType, false), //9
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_TYPE.columnName, TobaccoUseColumn.DATA_VALUE_TYPE.dataType, false), //10
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE.columnName, TobaccoUseColumn.DATA_VALUE.dataType, false), //11
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_FOOTNOTE_SYMBOL.columnName, TobaccoUseColumn.DATA_VALUE_FOOTNOTE_SYMBOL.dataType, false), //12
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_FOOTNOTE.columnName, TobaccoUseColumn.DATA_VALUE_FOOTNOTE.dataType, false), //13
                DataTypes.createStructField(TobaccoUseColumn.DATA_VALUE_STD_ERR.columnName, TobaccoUseColumn.DATA_VALUE_STD_ERR.dataType, false), //14
                DataTypes.createStructField(TobaccoUseColumn.LOW_CONFIDENCE_LIMIT.columnName, TobaccoUseColumn.LOW_CONFIDENCE_LIMIT.dataType, false), //15
                DataTypes.createStructField(TobaccoUseColumn.HIGH_CONFIDENCE_LIMIT.columnName, TobaccoUseColumn.HIGH_CONFIDENCE_LIMIT.dataType, false), //16
                DataTypes.createStructField(TobaccoUseColumn.SAMPLE_SIZE.columnName, TobaccoUseColumn.SAMPLE_SIZE.dataType, false), //17
                DataTypes.createStructField(TobaccoUseColumn.GENDER.columnName, TobaccoUseColumn.GENDER.dataType, false), //18
                DataTypes.createStructField(TobaccoUseColumn.RACE.columnName, TobaccoUseColumn.RACE.dataType, false), //19
                DataTypes.createStructField(TobaccoUseColumn.AGE.columnName, TobaccoUseColumn.AGE.dataType, false), //20
                DataTypes.createStructField(TobaccoUseColumn.EDUCATION.columnName, TobaccoUseColumn.EDUCATION.dataType, false), //21
                DataTypes.createStructField(TobaccoUseColumn.GEOLOCATION.columnName, TobaccoUseColumn.GEOLOCATION.dataType, false), //22
                DataTypes.createStructField(TobaccoUseColumn.TOPIC_TYPE.columnName, TobaccoUseColumn.TOPIC_TYPE.dataType, false), //23
                DataTypes.createStructField(TobaccoUseColumn.TOPIC_ID.columnName, TobaccoUseColumn.TOPIC_ID.dataType, false), //24
                DataTypes.createStructField(TobaccoUseColumn.MEASURE_ID.columnName, TobaccoUseColumn.MEASURE_ID.dataType, false), //25
                DataTypes.createStructField(TobaccoUseColumn.STRATIFICATION_ID1.columnName, TobaccoUseColumn.STRATIFICATION_ID1.dataType, false), //26
                DataTypes.createStructField(TobaccoUseColumn.STRATIFICATION_ID2.columnName, TobaccoUseColumn.STRATIFICATION_ID2.dataType, false), //27
                DataTypes.createStructField(TobaccoUseColumn.STRATIFICATION_ID3.columnName, TobaccoUseColumn.STRATIFICATION_ID3.dataType, false), //28
                DataTypes.createStructField(TobaccoUseColumn.STRATIFICATION_ID4.columnName, TobaccoUseColumn.STRATIFICATION_ID4.dataType, false), //29
                DataTypes.createStructField(TobaccoUseColumn.SUB_MEASURE_ID.columnName, TobaccoUseColumn.SUB_MEASURE_ID.dataType, false), //30
                DataTypes.createStructField(TobaccoUseColumn.DISPLAY_ORDER.columnName, TobaccoUseColumn.DISPLAY_ORDER.dataType, false), //30
            )
        )
    }

    private fun cleanDataFrame(dataFrame: Dataset<Row>): Dataset<Row> {
        return dataFrame
            .withColumn(
                TobaccoUseColumn.DATA_VALUE.columnName,
                `when`(
                    trim(col(TobaccoUseColumn.DATA_VALUE.columnName))
                        .cast("double")
                        .isNotNull,
                    trim(col(TobaccoUseColumn.DATA_VALUE.columnName))
                        .cast("double")
                ).otherwise(null)
            )
            .withColumn(
                TobaccoUseColumn.SAMPLE_SIZE.columnName,
                `when`(
                    trim(col(TobaccoUseColumn.SAMPLE_SIZE.columnName))
                        .cast("double")
                        .isNotNull,
                    trim(col(TobaccoUseColumn.SAMPLE_SIZE.columnName))
                        .cast("double")
                ).otherwise(null)
            )
            .filter(col(TobaccoUseColumn.DATA_VALUE.columnName).isNotNull)
            .filter(col(TobaccoUseColumn.SAMPLE_SIZE.columnName).isNotNull)
//            .filter(col(TobaccoUseColumn.AGE.columnName).notEqual("All Ages"))
    }

    private fun getPercentageUsageSortedByState(filteredDataFrame: Dataset<Row>): Dataset<Row> {

        // Debug unique LocationDesc values
        filteredDataFrame.select(
            col(TobaccoUseColumn.LOCATION_DESC.columnName),
            col(TobaccoUseColumn.DATA_VALUE.columnName),
            col(TobaccoUseColumn.SAMPLE_SIZE.columnName)
        ).distinct().show()

        // Group by LocationDesc and calculate weighted average
        val sortedByStatePercentageUsage = filteredDataFrame
            .groupBy(col(TobaccoUseColumn.LOCATION_DESC.columnName))
            .agg(
                sum(
                    col(TobaccoUseColumn.DATA_VALUE.columnName)
                        .divide(100) // Convert percentage to decimal
                        .multiply(col(TobaccoUseColumn.SAMPLE_SIZE.columnName))
                ).divide(
                    sum(col(TobaccoUseColumn.SAMPLE_SIZE.columnName))
                ).alias("Weighted_Average_Tobacco_Use")
            )
            .orderBy(desc("Weighted_Average_Tobacco_Use"))

        return sortedByStatePercentageUsage
    }

    private fun getPercentageUsageSortedByAge(filteredDataFrame: Dataset<Row>): Dataset<Row> {

        // Debug unique AGE values
        filteredDataFrame.select(
            col(TobaccoUseColumn.AGE.columnName),
            col(TobaccoUseColumn.DATA_VALUE.columnName),
            col(TobaccoUseColumn.SAMPLE_SIZE.columnName)
        ).distinct().show()

        // Group by AGE and calculate weighted average
        val sortedByAgePercentageUsage = filteredDataFrame
            .groupBy(col(TobaccoUseColumn.AGE.columnName))
            .agg(
                sum(col(TobaccoUseColumn.DATA_VALUE.columnName)
                    .divide(100) // Convert percentage to decimal
                    .multiply(col(TobaccoUseColumn.SAMPLE_SIZE.columnName)))
                    .divide(
                        sum(col(TobaccoUseColumn.SAMPLE_SIZE.columnName))
                    )
                    .alias("Weighted_Average_Tobacco_Use")
            )
            .orderBy(desc("Weighted_Average_Tobacco_Use"))

        return sortedByAgePercentageUsage
    }

    private fun getTopTen(data: Dataset<Row>): String {
        val topTenByAge = data.limit(10).collect() as Array<Row>
        val topTenByAgeString = "- " + topTenByAge.joinToString(separator = "\n- ") { row: Row ->
            "${row.getString(0)}: %.1f%%".format(row.getDouble(1) * 100)
        }
        return topTenByAgeString
    }

    private fun returnResultFormatted(
        rows: List<Row>,
        maleCount: Long,
        femaleCount: Long,
        overallCount: Long,
        topStatesString: String,
        topTenByAgeString: String
    ) = """
Number of rows in data set: ${rows.size}
- Male gender count: $maleCount
- Female gender count: $femaleCount
- Overall gender count: $overallCount
    
Top 10 States by Average Percentage of Usage
$topStatesString

Overview of Percentage Usage by Age:
$topTenByAgeString
    """.trimIndent()
}