package com.pavelkostal.sparkdataanalysingdemoproject.controller

import com.pavelkostal.sparkdataanalysingdemoproject.service.DataAnalyse
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@RestController
@RequestMapping("api/v1/analysis")
class PageController (val dataAnalyse: DataAnalyse){

    @GetMapping("/test")
    fun homePage(): String {
        val currentTime = LocalDateTime.now()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn")
        val formattedTime = currentTime.format(formatter)
        return "Current time: $formattedTime"
    }

    @GetMapping("/analyze-data")
    fun analyzeDataFromCsv(): String {
        return dataAnalyse.analyzeDataFromCsvFromInternalFile()
    }

    @PostMapping("/analyze-data")
    fun analyzeDataFromCsv(@RequestParam("file") file: MultipartFile): String {
        if (file.isEmpty) {
            throw IllegalArgumentException("No file uploaded or file is empty.")
        }

        return dataAnalyse.analyzeDataFromCsvFromExternalFile(file)
    }

}