package com.pavelkostal.sparkdataanalysingdemoproject.service

import org.springframework.web.multipart.MultipartFile

interface DataAnalyse {

    fun analyzeDataFromCsvFromInternalFile():String

    fun analyzeDataFromCsvFromExternalFile(file: MultipartFile):String
}