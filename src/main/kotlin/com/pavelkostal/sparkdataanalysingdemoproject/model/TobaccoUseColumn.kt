package com.pavelkostal.sparkdataanalysingdemoproject.model

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes

enum class TobaccoUseColumn(val columnName: String, val description: String, val apiFieldName: String, val dataType: DataType) {
    YEAR("YEAR", "Year", "year", DataTypes.StringType),
    DATA_VALUE_TYPE("Data_Value_Type", "Type of data (yes/no, percentage, dollar, etc)", "data_value_type", DataTypes.StringType),
    DATA_VALUE("Data_Value", "Value of the data", "data_value", DataTypes.DoubleType),
    DATA_VALUE_FOOTNOTE_SYMBOL("Data_Value_Footnote_Symbol", "Where applicable, this text is associated with the footnote symbol", "data_value_footnote_symbol", DataTypes.StringType),
    DATA_VALUE_FOOTNOTE("Data_Value_Footnote", "Where applicable, this text is associated with the footnote text", "data_value_footnote", DataTypes.StringType),
    DATA_VALUE_STD_ERR("Data_Value_Std_Err", "Standard error for the data value", "data_value_std_err", DataTypes.DoubleType),
    LOW_CONFIDENCE_LIMIT("Low_Confidence_Limit", "Confidence interval lower limit", "low_confidence_limit", DataTypes.DoubleType),
    HIGH_CONFIDENCE_LIMIT("High_Confidence_Limit", "Confidence interval upper limit", "high_confidence_limit", DataTypes.DoubleType),
    SAMPLE_SIZE("Sample_Size", "Sample size", "sample_size", DataTypes.DoubleType),
    GENDER("Gender", "Gender of respondents", "gender", DataTypes.StringType),
    RACE("Race", "Race of respondents", "race", DataTypes.StringType),
    LOCATION_ABBR("LocationAbbr", "Location abbreviation", "locationabbr", DataTypes.StringType),
    AGE("Age", "Age of respondents", "age", DataTypes.StringType),
    EDUCATION("Education", "Education level of respondents", "education", DataTypes.StringType),
    GEOLOCATION("GeoLocation", "Geolocation codes for mapping purposes", "geolocation", DataTypes.StringType),
    TOPIC_TYPE_ID("TopicTypeId", "Topic type identifier code - can be used for filtering; used in application", "topictypeid", DataTypes.StringType),
    TOPIC_ID("TopicId", "Topic identifier code - can be used for filtering; used in application", "topicid", DataTypes.StringType),
    MEASURE_ID("MeasureId", "Measure identifier - can be used for filtering", "measureid", DataTypes.StringType),
    STRATIFICATION_ID1("StratificationID1", "Gender identifier code - can be used for filtering; used in application", "stratificationid1", DataTypes.StringType),
    STRATIFICATION_ID2("StratificationID2", "Age identifier code - can be used for filtering; used in application", "stratificationid2", DataTypes.StringType),
    STRATIFICATION_ID3("StratificationID3", "Race identifier code - can be used for filtering; used in application", "stratificationid3", DataTypes.StringType),
    STRATIFICATION_ID4("StratificationID4", "Education identifier code - can be used for filtering; used in application", "stratificationid4", DataTypes.StringType),
    LOCATION_DESC("LocationDesc", "Location description", "locationdesc", DataTypes.StringType),
    SUB_MEASURE_ID("SubMeasureID", "Submeasure identifier code - can be used for filtering; used in application", "submeasureid", DataTypes.StringType),
    DISPLAY_ORDER("DisplayOrder", "Display order; used in application", "displayorder", DataTypes.StringType),
    TOPIC_TYPE("TopicType", "Type of topic", "topictype", DataTypes.StringType),
    TOPIC_DESC("TopicDesc", "Topic description", "topicdesc", DataTypes.StringType),
    MEASURE_DESC("MeasureDesc", "Measure description", "measuredesc", DataTypes.StringType),
    DATA_SOURCE("DataSource", "Data source", "datasource", DataTypes.StringType),
    RESPONSE("Response", "Response categories", "response", DataTypes.StringType),
    DATA_VALUE_UNIT("Data_Value_Unit", "Indicator of the type of data value ($, %, etc)", "data_value_unit", DataTypes.StringType);

    companion object {
        fun fromColumnName(columnName: String): TobaccoUseColumn? {
            return values().find { it.columnName == columnName }
        }
    }
}