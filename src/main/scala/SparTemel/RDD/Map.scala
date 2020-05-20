package SparTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Map {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val sc = spark.sparkContext

    println("\n \n ************* MAP İLE PAIR RDD YARATMAK ve İPTAL OLAN SATIŞLARIN TOPLAM TUTARINI HESAPLAMAK *********")

    val rawData = sc.textFile("/home/kemal/Downloads/OnlineRetail.csv").filter(!_.contains("InvoiceNo"))
    val mapData = rawData.map(x => {
      var total: Double = x.split(";")(3).toDouble * x.split(";")(5).replace(",", ".").toDouble
      var cancelProduct: Boolean = x.startsWith("C")
      (cancelProduct, total)

    }).filter(_._1.!=(false))
    mapData.reduceByKey(_ + _)
      .map(_._2)
      .foreach(println)


  }


}
