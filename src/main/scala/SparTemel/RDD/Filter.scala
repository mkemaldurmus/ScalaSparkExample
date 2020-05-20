package SparTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Filter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    val sc = spark.sparkContext

    val rawData = sc.textFile("/home/kemal/Downloads/OnlineRetail.csv")
    val withoutTitle = rawData.mapPartitionsWithIndex((number, iter) => if (number == 0) iter.drop(1) else iter)
    //başlıksız satır sayısı
    // print(withoutTitle.count)
    rawData.take(10).foreach(println)


    println("**********  Birim fiyatı 30'dan büyük olanları filtrele   **************")
    val price: RDD[String] = withoutTitle.filter(_.split(";")(5).trim.replace(",", ".").toFloat > 30.0)
    //price.take(10).foreach(println)


    println("********** Ürün tanımı içinde COFFEE geçenleri ve birim fiyatı 20.0'den büyükleri filtrele   **************")

    val coffeeResult: RDD[String] = withoutTitle.filter(line => line.contains("COFFEE") && line.split(";")(5).trim.replace(",", ".").toFloat > 20.0)

    coffeeResult.take(10).foreach(println)
  }

}
