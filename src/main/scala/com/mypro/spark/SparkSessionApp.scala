package com.mypro.spark

import org.apache.spark.sql.SparkSession


object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sparkSessionApp").master("local[2]").getOrCreate()
    val people = spark.read.json("E:\\workspace\\study\\SparkSQLProject\\src\\main\\resources\\testjson.json")
    people.show()
    spark.stop()
  }
}
