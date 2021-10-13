package com.murphy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.FileInputStream
import java.net.URI
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties
import scala.util.{Failure, Success, Try}

object DataIngestion {
  def main(args: Array[String]): Unit = {

    val log: Logger = LogManager.getRootLogger
    val prop: Properties = new Properties()
    val spark = SparkSession.builder()
      .appName("DataIngestionLam")
      .master("local")
      .getOrCreate()

    val configProps = args(0)
    val srcFile = args(1)
    val target = args(2)
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(60))


    spark.sparkContext.setLogLevel("WARN")
    log.info("JSON File Load Start !!")

    prop.load(new FileInputStream(configProps))
    val zookeeperUrl = prop.getProperty("zkUrl")
    val metaTableName = prop.getProperty("meta.tableName")
    val tsTableName = prop.getProperty("timeSeries.tableName")
    val fileProcTableName = prop.getProperty("fileProc.tableName")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "atlhashed01.hashmap.net:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("files") //topics list
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val stream: DStream[String] = kafkaStream.map(x => x.value())
    //stream.print()

    def getFileList(path: String): List[String] = {
      val uri = new URI("hdfs:///")
      val fs = FileSystem.get(uri, new Configuration())
      val filePath = new Path(path)
      val files = fs.listFiles(filePath, false)
      var fileList = List[String]()

      while (files.hasNext) {
        fileList ::= files.next().getPath.toString
      }
      fileList
    }

    val filelist = getFileList(srcFile)
    stream.foreachRDD(x => {
      val collect = x.collect()

      for (s: String <- collect) {
        System.out.println(s)
        println("+++++++++++++")

        filelist.filter(x => x.contains(s)).foreach(x => {
          val df = spark.read.option("multiline", "true")
            .option("header", "true")
            .option("inferSchema", "true")
            .json(x)

          val cols = df.columns.toList
          if (cols.contains("meta") && cols.contains("recipe_steps"))
            OES(df, spark, log)
          else
            WDL(df, spark, log)
          moveFileToArchive(x, target)
        })

        def moveFileToArchive(inPath: String, toPath: String): Unit = {
          val uri = new URI("hdfs:///")
          val file = inPath.split("/")
          val filename = file(file.length - 1)
          val fs = FileSystem.get(uri, new Configuration())
          val sourcePath = new Path(inPath)
          val destPath = new Path(toPath + "/" + filename)
          println(sourcePath)
          println(destPath)
          fs.rename(sourcePath, destPath)
          // println("moved")
        }

        def WDL(inputDf: DataFrame, spark: SparkSession, log: Logger): Unit = {
          import spark.implicits._
          val metaPKDF = inputDf.select(col("process_executions.header.process_job_ID")
            .alias("PJID"), col("process_executions.header.wafer_ID")
            .alias("WID"), col("process_executions.header.lot_ID")
            .alias("LID"), col("process_executions.header.slot_ID")
            .alias("SLTID"), col("process_executions.header.recipe_ID")
            .alias("RCPID"), col("process_executions.header.tool_ID")
            .alias("TID"), col("process_executions.header.chamber_ID")
            .alias("CBRN"), col("process_executions.header.start_time")
            .alias("RSTS"))

          val metadataWithFNDF = metaPKDF.withColumn("MMID", hash(metaPKDF.columns.map(col): _*))
            .withColumn("WDFLN", lit(inputDf.inputFiles(0)))

          val metadataDF: DataFrame = metadataWithFNDF.select(col("PJID").getItem(0).alias("PJID"),
            col("WID").getItem(0).alias("WID"),
            col("LID").getItem(0).alias("LID"),
            col("SLTID").getItem(0).cast(IntegerType).alias("SLTID"),
            col("MMID"),
            col("RCPID").getItem(0).alias("RCPID"),
            col("TID").getItem(0).alias("TID"),
            col("CBRN").getItem(0).alias("CBRN"),
            col("WDFLN"),
            col("RSTS").getItem(0).cast("timestamp").alias("RSTS"))

          metadataDF.persist
          val aryMMID = metadataDF.limit(1).select("MMID").as[Integer].collect()
          val intMMID = aryMMID(0)

          //Save to Phoenix
          //    val zookeeperUrl = "jdbc:phoenix:atlhashdn01.hashmap.net,atlhashdn02.hashmap.net,atlhashdn03.hashmap.net:2181"
          //    val metaTableName = "MATERIAL_META1"
          //    val tsTableName = "TS_T1"


          try {
            metadataDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", metaTableName).option("zkUrl", zookeeperUrl).save()
          } catch {
            case ph: Throwable => log.error("Unable to write data into Phoenix table " + metaTableName + ". Exception: " + ph.getMessage)
          }


          log.info("Meta Data is inserted into Phoenix!")

          val fileProcDF: DataFrame = metadataDF.select($"PJID", $"WID", $"SID").withColumn("ITD", lit(current_timestamp())).withColumn("PR", lit(null).cast(BooleanType)).withColumn("PTD", lit(null).cast(TimestampType))

          try {
            fileProcDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", fileProcTableName).option("zkUrl", zookeeperUrl).save()
          } catch {
            case ph: Throwable => log.error("Unable to write data into Phoenix table " + metaTableName + ". Exception: " + ph.getMessage)
          }


          val arrayTSDF0 = inputDf.select(col("process_executions.data").getItem(0).alias("data1"))
          val arrayTSDF1 = arrayTSDF0.select(explode(col("data1")))

          val arrayTSDF2 = arrayTSDF1.withColumn("TSData", struct(col("col.step").alias("tsstep"), col("col.parameters").alias("tsParameters"))).select("TSData")
          val arrayTSDF3 = arrayTSDF2.select(col("TSData.tsstep").alias("tsstep"), col("TSData.tsParameters").alias("tsParameters"))
          val arrayTSDF4 = arrayTSDF3.select(col("tsstep"), explode(col("tsParameters")).alias("tsParameters"))
          val arrayTSDF5 = arrayTSDF4.select(col("tsstep"), col("tsParameters.parameter_name").alias("tsParaName"), col("tsParameters.samples").alias("tsSamples"))
          val arrayTSDF6 = arrayTSDF5.select(col("tsstep"), col("tsParaName"), explode(col("tsSamples")).alias("tsSamples"))
          val arrayTSDF7 = arrayTSDF6.select(col("tsstep").alias("STPID"), col("tsParaName").alias("CID"), col("tsSamples.time").cast(TimestampType).alias("TS"), col("tsSamples.value").cast(DoubleType).alias("VL"))
          val arrayTSDF8 = arrayTSDF7.withColumn("MMID", lit(intMMID))

          val finalTSDF = arrayTSDF8.select(col("MMID"), col("CID"), col("TS"), col("VL"), col("STPID"))
          finalTSDF.show(5, truncate = false)
          try {
            finalTSDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", tsTableName).option("zkUrl", zookeeperUrl).save()
          } catch {
            case ph: Throwable => log.error("Unable to write data into Phoenix table " + tsTableName + ". Exception: " + ph.getMessage)
          }


          log.info("Time Series Data is inserted into Phoenix!")

          log.info("WDL Ingestion is Completed !!")
        }

        def OES(inputDf: DataFrame, spark: SparkSession, log: Logger): Unit = {
          import spark.implicits._

          val getTimestamp: String => Option[Timestamp] = {
            case "" => None
            case s =>
              val format = new SimpleDateFormat("dd-MM-yyyy'T'HH:mm:ss.SSS'Z'")
              Try(new Timestamp(format.parse(s).getTime)) match {
                case Success(t) => Some(t)
                case Failure(_) => None
              }
          }
          val getTimestampUDF = udf(getTimestamp)
          val metaPKDF = inputDf.select(col("meta.lot_ID").alias("LID"), col("meta.slot_ID").alias("SLTID").cast(IntegerType), col("meta.wafer_ID").alias("WID"), col("meta.recipe_ID").alias("RCPID"), col("meta.tool_ID").alias("TID"), col("meta.chamber_id").alias("CBRN"), col("meta.process_job_ID").alias("PJID"), col("meta.start_time").alias("RSTS"))
          val metadataWithFNDF = metaPKDF.withColumn("MMID", hash(metaPKDF.columns.map(col): _*)).withColumn("WDFLN", lit(inputDf.inputFiles(0))).withColumn("SID", lit(1))

          val metadataDF: DataFrame = metadataWithFNDF.select(col("PJID"), col("WID"), col("SID"), col("LID"), col("SLTID").cast(IntegerType).alias("SLTID"), col("MMID"), col("RCPID"), col("TID"), col("CBRN"), col("WDFLN"), col("RSTS").cast(TimestampType).alias("RSTS"))
          metadataDF.persist
          val aryMMID = metadataDF.limit(1).select("MMID").as[Integer].collect()
          val intMMID = aryMMID(0)
          val fileProcDF: DataFrame = metadataDF.select($"PJID", $"WID", $"SID").withColumn("ITD", lit(current_timestamp())).withColumn("PR", lit(null).cast(BooleanType)).withColumn("PTD", lit(null).cast(TimestampType))
          try {
            metadataDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", metaTableName).option("zkUrl", zookeeperUrl).save()
          } catch {
            case ph: Throwable => log.error("Unable to write data into Phoenix table " + metaTableName + ". Exception: " + ph.getMessage)
          }


          try {
            fileProcDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", fileProcTableName).option("zkUrl", zookeeperUrl).save()
          } catch {
            case ph: Throwable => log.error("Unable to write data into Phoenix table " + metaTableName + ". Exception: " + ph.getMessage)
          }
          log.info("Meta Data is inserted into Phoenix!")

          val wavelengthDF = inputDf.select(col("recipe_steps.OES_spectra.Wavelengths").alias("Wavelengths"), col("recipe_steps.OES_spectra.Spectra").alias("arrayVal"), col("recipe_steps.Step").alias("step"))
          val arrayDataDF0 = wavelengthDF.select(col("Wavelengths"), col("arrayVal")).withColumn("Wavelengths", col("Wavelengths")(0))
          val arrayDataDF1 = arrayDataDF0.select(col("Wavelengths"), explode(col("arrayVal")).as("arrayVal"))
          val arrayDataDF2 = arrayDataDF1.select(col("Wavelengths"), explode(col("arrayVal")).as("arrayVal"))

          val arrayDataDF3 = arrayDataDF2.select(col("Wavelengths"), col("arrayVal.Step").alias("Step"), col("arrayVal.Time").alias("Time"), col("arrayVal.Values").alias("Values")).withColumn("Time", getTimestampUDF(col("Time"))) //.withColumn("Time", unix_timestamp($"Time", "dd-MM-yyyy'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
          val arrayDataDF4 = arrayDataDF3.withColumn("valuesExploded", explode(arrayDataDF3("Values"))).select(col("Step").as("Step"), col("Time").as("Time"), col("valuesExploded").as("Values"))
          val arrayDataDF5 = arrayDataDF3.withColumn("wavelengthExploded", explode(arrayDataDF3("Wavelengths"))).select(col("Step").as("Step1"), col("Time").as("Time1"), col("wavelengthExploded").as("Wavelengths"))

          val arrayDataDF44 = arrayDataDF4.withColumn("index", monotonically_increasing_id)
          val arrayDataDF55 = arrayDataDF5.withColumn("index", monotonically_increasing_id)

          val arrayDataDF6 = arrayDataDF44.join(arrayDataDF55, arrayDataDF44("index") === arrayDataDF55("index"), "outer").drop("index").drop("Step1").drop("Time1")

          val arrayDataDF7 = arrayDataDF6.select(col("Step").cast(StringType).alias("STPID"), col("Wavelengths").cast(StringType).alias("CID"), col("Time").alias("TS"), col("Values").cast(DoubleType).alias("VL"))
          val arrayDataDF8 = arrayDataDF7.withColumn("MMID", lit(intMMID))

          val finalTSDF = arrayDataDF8.select(col("MMID"), col("CID"), col("TS"), col("VL"), col("STPID"))
          finalTSDF.show(20, truncate = false)

          try {
            finalTSDF.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", tsTableName).option("zkUrl", zookeeperUrl).save()
          } catch {
            case ph: Throwable => log.error("Unable to write data into Phoenix table " + tsTableName + ". Exception: " + ph.getMessage)
          }

          finalTSDF.show(5, truncate = false)
          log.info("Time Series Data is inserted into Phoenix!")

          log.info("OES Ingestion is Completed !!")
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
