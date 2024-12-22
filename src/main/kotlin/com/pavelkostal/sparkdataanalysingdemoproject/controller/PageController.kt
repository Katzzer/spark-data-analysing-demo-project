package com.pavelkostal.sparkdataanalysingdemoproject.controller

import com.pavelkostal.sparkdataanalysingdemoproject.service.DataAnalyse
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile

@RestController
@RequestMapping("api/v1/analysis")
class PageController (val dataAnalyse: DataAnalyse){

    @GetMapping("/test")
    fun homePage(): String {
        return "All work"
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