import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object day2 {
  def main(args: Array[String]): Unit = {
            val spark = SparkSession
              .builder()
              .appName("practice")
              .master("local")
              .getOrCreate()

            val dff=spark.read.textFile("C:\\Users\\nk224\\Downloads\\spark_readme.txt").rdd
            val ans=dff.flatMap(x=>x.split(" ")).map(word=>(word,1)).reduceByKey(_ + _)
            //val ans1=dff.flatMap(x=>x.split(" ")).map(word=>(word,1)).countByKey()
            val ans2=dff.flatMap(x=>x.split(" ")).map(word=>(word,1)).groupByKey().map(t => (t._1, t._2.sum))


            for (elem <- ans.collect()) {
              println(elem)
            }
            println()
        //    for (elem <- ans1.collect()) {
        //      print(elem)
        //    }
            println()
            for (elem <- ans2.collect()) {
              println(elem)
            }


            println(dff.getNumPartitions)

            dff.repartition(2)
            println(dff.getNumPartitions)

            dff.repartition(3)
            println(dff.getNumPartitions)

            val freqmorethan3=ans.filter(x=>x._2>3)
            for (elem <- freqmorethan3.collect()) {
              println(elem)
            }
            println()

            val lengthmorethan3=ans.filter(x=>x._1.length>3)
            for (elem <- lengthmorethan3.collect()) {
              println(elem)
            }
            println()

            val wordsContainingComma=ans.filter(x=>x._1.contains(","))
            for (elem <- wordsContainingComma.collect()) {
              println(elem)
            }
            println()

            val replaceComma=wordsContainingComma.map(x=>x._1.replaceAll(",","|"))
            for (elem <- replaceComma.collect()) {
              println(elem)
            }
            println()

        val data = Array(1, 2, 3, 4, 5, 6, 7, 8)
        val conf = new SparkConf()
          .setMaster("local[2]")
          .setAppName("CountingSheep")
        val sc = new SparkContext(conf)
        val dff2 = sc.parallelize(data)

        val sum = dff2.collect().sum
        print(sum)


    val spark1 = SparkSession
      .builder()
      .appName("practice")
      .master("local")
      .getOrCreate()

    val dff1 = spark1.read.textFile("C:\\Users\\nk224\\Downloads\\server_log.txt").rdd
    val flatdff=dff1.flatMap(x => x.split(" "))
    val errormesages=flatdff.filter(_.contains("ERROR")).collect()


    for(i<-errormesages)
      println(i)


    println("///////")
    val mysqlphp=flatdff.filter(x=>x.contains("php")||x.contains("mysql")).collect().length

    println("no of wordds contais mysql php"+mysqlphp)


    flatdff.filter(x=>x.contains("RailsApp")).take(2).foreach(println)

  }
}
