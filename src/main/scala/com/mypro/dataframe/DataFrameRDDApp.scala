package com.mypro.dataframe

import org.apache.spark.sql.SparkSession


object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    // RDD => DataFrame
    var rdd = spark.sparkContext
      .textFile("E:\\workspace\\study\\imooclogspark\\src\\main\\resources\\people.txt")

    import spark.implicits._

    val peopleDf = rdd.map(_.split(","))
        .map(line => Info(
          line(0).toInt,
          line(1),
          line(2).toInt))
        .toDF()
    peopleDf.show()

//    peopleDf.filter(peopleDf.col("age")>30).show
    peopleDf.createOrReplaceTempView("people")
    spark.sql("select * from people where age >30").show()
    spark.sql("show tables").show()

    spark.close()
  }

  case class Info(id: Int, name: String, age: Int)

}
