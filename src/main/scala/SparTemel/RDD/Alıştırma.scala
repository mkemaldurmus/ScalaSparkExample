package SparTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Alıştırma {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("Test")
      .config("spark.executor.memory", "3g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()
    val sc = spark.sparkContext

    val list = List(2, 3, 7, 13, 15, 22, 36, 7, 11, 3, 25)
    val raw=sc.parallelize(list)
    val frequency =raw.map(x=>(x,1)).reduceByKey(_ + _ )
    frequency.sortBy(_._2,false).take(10).foreach(println)

    //val stringRaw=new String("Spark'ı öğrenmek çok heyecan verici")
   // val stringMAp: Array[String] =stringRaw.split(" ").map(_.toUpperCase())
   // val rawData = sc.textFile("/home/kemal/Desktop/test.txt")
    //val count= rawData.count()
   // val count=rawData.flatMap(_.split(" ")).count()
    //stringMAp.foreach(println)

  }

}
