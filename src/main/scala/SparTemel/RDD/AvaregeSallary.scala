package SparTemel.RDD


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object AvaregeSallary {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val context: SparkContext = spark.sparkContext
    val rawData = context.textFile("/home/kemal/Downloads/simple.csv").filter(!_.contains("sirano"))
    print("\n---------------Mesaleğe göre ortalma maaş------------------------")

    val rawMap = rawData.map(line => {
      val jobs = line.split(",")(3)
      val salary = line.split(",")(5).toDouble
      (jobs, salary)
    }).mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2).sortBy(_._2, ascending = false)
    rawMap.collect.foreach(println)


  }
}
