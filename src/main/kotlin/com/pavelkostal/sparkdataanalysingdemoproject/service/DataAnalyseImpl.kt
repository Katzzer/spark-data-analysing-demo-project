package com.pavelkostal.sparkdataanalysingdemoproject.service

import com.pavelkostal.sparkdataanalysingdemoproject.model.SparkColumn
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.springframework.stereotype.Component
import org.springframework.web.multipart.MultipartFile

@Component
class DataAnalyseImpl : DataAnalyse {
    override fun analyzeDataFromCsv(file: MultipartFile): String {
        // Convert the uploaded CSV file to a list of strings (one string for each line)
        val csvData = file.inputStream.bufferedReader().readLines()

        val spark = SparkSession.builder() // Run Apache Spark locally
            .appName("Kotlin Spark Example")
            .master("local[*]") // For local development without master and worker node running in Docker
            .getOrCreate()

        val javaSparkContext = JavaSparkContext(spark.sparkContext())

        // Check if CSV data is empty or invalid
        if (csvData.isEmpty()) {
            return "Uploaded CSV is empty."
        }

        val header = csvData.first() // First row is the header
        val data = csvData.drop(1)   // Exclude the header row

        // Check if data contains rows
        if (data.isEmpty()) {
            return "No data rows present in the CSV."
        }

        // Define schema (assuming YEAR is stored as IntegerType)
        val schema = StructType(arrayOf(
            DataTypes.createStructField("YEAR", DataTypes.IntegerType, false)
        ))

        // Preprocess rows to handle invalid YEAR values
        val rows = data.mapNotNull { line ->
            val split = line.split(",").map { it.trim() }
            val yearString = split[0] // Assuming the year is the first column

            val year: Int? = try {
                // Handle ranges like "2020-2021" by taking the first number
                if (yearString.contains("-")) {
                    yearString.split("-")[0].toInt() // Take the first year in the range
                } else {
                    yearString.toInt() // Parse as a single integer
                }
            } catch (e: NumberFormatException) {
                null // Skip invalid rows (e.g., non-numeric strings)
            }

            year?.let { RowFactory.create(it) } // Return a Row if the year is valid, otherwise null
        }

        // Skip if rows are empty after preprocessing
        if (rows.isEmpty()) {
            return "No valid data rows present in the CSV."
        }

        val rowRDD = javaSparkContext.parallelize(rows)

        val dataFrame = spark.createDataFrame(rowRDD, schema)

        val averageYear = dataFrame
            .select(avg(SparkColumn.YEAR.columnName))
            .first()
            ?.getDouble(0) ?: 0.0

        return "Number of rows: ${rows.size}\nAverage year: $averageYear"
    }
}