package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Try

object BostonCrimesMap {
  def main (args: Array[String]): Unit ={
    if (args.length != 3) {
      System.err println "Usage com.example.BostonCrimesMap {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder} "
      System.exit(-1)
    }
    val pathToCrime = args(0)
    val pathToOffenseCodes = args(1)
    val pathToOutFolder = args(2)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("BostonCrimesMap - Scala App")
      .getOrCreate()
    import spark.implicits._
    val dfCrime = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(pathToCrime)
    val dfOffenseCodes = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(pathToOffenseCodes)
    //Формируем короткое имя NAME_SHORT, разрезая по -
    val get_head = udf((xs: Seq[String]) => Try(xs.head).toOption)
    val dfOffenseCodesN = dfOffenseCodes.withColumn("crime_type", get_head(split(str = col("NAME"),pattern = "-")))
    // Собираем основной DF, объединив Crime со справочником dfOffenseCodesN
    dfCrime.as("a").join(broadcast(dfOffenseCodesN).as("b")).where($"a.OFFENSE_CODE"===$"b.CODE")
      .drop(dfOffenseCodesN("CODE")).createOrReplaceTempView("crime")


    spark.sql("""
      select t.DISTRICT,  percentile_approx(t.cnt,0.5) crimes_monthly  from
       (select DISTRICT, count(INCIDENT_NUMBER) cnt from crime group by DISTRICT, YEAR, MONTH) t
       group by t.DISTRICT
      """).createOrReplaceTempView("crimes_monthly ")
    spark.sql("""
       select DISTRICT, count(INCIDENT_NUMBER) crimes_total, avg(Lat) lat, avg(Long) lng from crime
        group by DISTRICT
       """).createOrReplaceTempView("crime_base")
    spark.sql("""
       select tt.DISTRICT, concat_ws(", ", collect_list(tt.crime_type)) as frequent_crime_types  from
        (select t.DISTRICT, t.crime_type, row_number() OVER (PARTITION BY t.DISTRICT ORDER BY t.cnt DESC) as rn from
        (select DISTRICT, crime_type, count(1) as cnt from crime group by DISTRICT, crime_type) t) tt
        where tt.rn < 4 group by tt.DISTRICT order by tt.DISTRICT
        """).createOrReplaceTempView("frequent_crime_types ")
    spark.sql(
      """
        select t1.DISTRICT, t1.crimes_total, t2.crimes_monthly, t3.frequent_crime_types, t1.lat, t1.lng from crime_base t1
         join crimes_monthly t2 on t1.DISTRICT = t2.DISTRICT
         join frequent_crime_types t3 on t1.DISTRICT = t3.DISTRICT
        """).repartition(1).write.parquet(pathToOutFolder)
      }
}
