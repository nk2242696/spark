package com.murphy

import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object MobileDashboard {



  import org.apache.spark.sql.functions.{col, lit, to_date, udf}
  import org.apache.spark.sql.{DataFrame, SQLContext}
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .master("local")
      .appName("Assignment")
      .enableHiveSupport()
      .getOrCreate()





    val NANRemover= udf(f = (str: String) => {
      if (str==null || str=="" ) 
        "-9999"
      else str
    })

    val bracketRemover= udf(f = (str: String) => {
      if (str != null && str.contains("\\(") ) {
        str.split("\\(")(0)
      }
      else str
    })

    val transformUppercase= udf(f = (str: String) => {
      if (str != null) {
        str.toUpperCase
      }
      else
      {
        str
      }
    })

    val yearExtractor= udf(f = (str: String) => {
      if (str != null && str.contains("/")) {
        val year=str.split("/")(2)
        year
      }
      else
      {
        str
      }
    })
    
    val waterQuality: DataFrame = spark.read.option("header","true")
                                            .option("inferSchema","true")
                                            .csv("file:///root/IndiaAffectedWaterQualityAreas.csv")

    val filtereWaterQuality = waterQuality.filter(!col("State Name").contains("*") && !col("District Name").contains("*") && !col("Block Name").contains("*") && !col("Panchayat Name").contains("*") && !col("Village Name").contains("*") && !col("Habitation Name").contains("*") && !col("Quality Parameter").contains("*") && !col("Year").contains("*"))

    val df1= filtereWaterQuality
      .withColumn("State Name",bracketRemover(col("State Name")))
      .withColumn("District Name", bracketRemover(col("District Name")))
      .withColumn("Block Name",bracketRemover(col("Block Name")))
      .withColumn("Panchayat Name",bracketRemover(col("Panchayat Name")))
      .withColumn("Village Name", bracketRemover(col("Village Name")))
      .withColumn("Habitation Name",bracketRemover(col("Habitation Name")))
      .withColumn("Quality Parameter", bracketRemover(col("Quality Parameter")))
      .withColumn("Year",bracketRemover(col("Year")))

    val districtCodes = spark.read.option("header","true")
                                  .option("inferSchema","true")
                                  .csv("file:///root/Districts Codes 2001.csv")
    val filteredDistrictCodes = districtCodes.filter(!col("Name of the State/Union territory and Districts").contains("*"))
                                        .withColumn("State Code" , NANRemover(col("State Code")))
                                        .withColumn("District Code", NANRemover(col("District Code")))
    val df2 = filteredDistrictCodes
      .withColumn("Name of the State/Union territory and Districts",transformUppercase(col("Name of the State/Union territory and Districts")))

    val joined1 = df1.join(df2,df1("State Name") === df2("Name of the State/Union territory and Districts")).drop("Name of the State/Union territory and Districts")
      val joined=joined1
      .withColumnRenamed("State Name","state_name")
      .withColumnRenamed("District Name", "district_name")
      .withColumnRenamed("State Code","state_code")
      .withColumnRenamed("District Code","district_code")
      .withColumnRenamed("Block Name", "block_Name")
      .withColumnRenamed("Panchayat Name", "panchayat_name")
      .withColumnRenamed("Village Name","village_name")
      .withColumnRenamed("Habitation Name", "habitaion_name")
      .withColumnRenamed("Quality Parameter","quality_parameter")

    joined.show()
    joined.write.format("orc").mode(SaveMode.Append).saveAsTable("db.tableA")

    val frequencyData = joined.groupBy("village_name","quality_parameter","Year").agg(count("quality_parameter").alias("frequency"))
    frequencyData.show()
    frequencyData.write.format("orc").mode(SaveMode.Append).saveAsTable("db.tableB")
  }

}

