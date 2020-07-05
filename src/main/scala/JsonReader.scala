package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import org.json4s._
import org.json4s.native.JsonMethods



case class WineMag(id: Option[Int] = None, country: Option[String] = None, points: Option[Int] = None, price: Option[Float] = None, title: Option[String] = None, variety: Option[String] = None, winery: Option[String] = None)

object JsonReader {


  def main (args: Array[String]): Unit ={
    if (args.length != 1) {
      System.err println "Usage com.example.JsonReader {path/to/winemag.json}"
      System.exit(-1)
    }
    val pathToWineMag = args(0)

    val JsonToWineMagF = (str: String) =>
    {
      implicit val formats = DefaultFormats
      JsonMethods.parse(str).extract[WineMag]
    }

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("JsonReader - Scala App")
      .getOrCreate()
    import spark.implicits._
    val JsonRDD :RDD[String] = spark.sparkContext.textFile(pathToWineMag)
    val jsonRDDCase: RDD[WineMag] = JsonRDD.map(x => JsonToWineMagF(x))
    jsonRDDCase.collect().foreach(println)

  }
}