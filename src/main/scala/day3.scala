
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object day3 extends App{

  val spark=SparkSession.builder().
    master("local").
    appName("day3").
    getOrCreate()

  val rdd=spark.sparkContext.
    textFile("C:\\Users\\nk224\\Downloads\\tagdata.txt")

  import spark.implicits._
  val schema = new StructType(Array(StructField("id",StringType,true),
    StructField("timestamp",StringType,true),
    StructField("val1",StringType,true),
    StructField("val2",StringType,true)))

  val data=rdd.map(_.split(",").toList)

  val data2=data.map(a=>Row.fromSeq(a))

  val dataframe=spark.createDataFrame(data2,schema)

  val castedDF=dataframe.select($"id",unix_timestamp($"timestamp", "MM/dd/yyyy HH:mm:ss")
    .cast(TimestampType).alias("date"),$"val1".cast(DoubleType),$"val2")

  val filteredDF=castedDF.filter($"id".isNotNull && $"date".isNotNull)
  filteredDF.show(castedDF.collect().length)
  filteredDF.cache()
  // val duplicateDF=filteredDF.filter()
}
