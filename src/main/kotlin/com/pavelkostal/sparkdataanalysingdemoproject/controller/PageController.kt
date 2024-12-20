package com.pavelkostal.sparkdataanalysingdemoproject.controller

import com.pavelkostal.sparkdataanalysingdemoproject.service.DataAnalyse
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile

@Controller
@RestController
@RequestMapping("api/v1/analysis")
class PageController (val dataAnalyse: DataAnalyse){

    @GetMapping("/demo")
    fun demo(): String {
        return "demo all works"
    }

    @PostMapping("/analyze-data")
    fun analyzeDataFromCsv(@RequestParam("file") file: MultipartFile): String {
        if (file.isEmpty) {
            throw IllegalArgumentException("No file uploaded or file is empty.")
        }

        return dataAnalyse.analyzeDataFromCsv(file)
    }

}