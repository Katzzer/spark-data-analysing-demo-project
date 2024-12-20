package com.pavelkostal.sparkdataanalysingdemoproject.service

import org.springframework.web.multipart.MultipartFile

interface DataAnalyse {

    fun analyzeDataFromCsv(file: MultipartFile):String
}