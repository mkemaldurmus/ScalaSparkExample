package SparTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountRDD {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val sc = spark.sparkContext

    val hikayeRDD = sc.textFile("/home/kemal/Desktop/original.txt")
    //print(hikayeRDD.count())
    val wordRDD = hikayeRDD.flatMap(word => word.split(" ")).filter(!_.contains("-"))
    val wordCount: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
    val result: RDD[(String, Int)] =wordCount.reduceByKey(_+_).sortBy(_._2,ascending = false)
    result.take(10).foreach(println)


  }
}
