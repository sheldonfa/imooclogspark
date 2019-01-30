package com.mypro.dataframe

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object DataFrameRDDApp2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    // RDD => DataFrame
    val url2 = this.getClass.getClassLoader.getResource("people.txt")
    println(url2)
    var rdd = spark.sparkContext
      .textFile(url2.toString)

    import spark.implicits._

    val peopleRdd = rdd.map(_.split(","))
      .map(line => Row(
        line(0).toInt,
        line(1),
        line(2).toInt))

    val structType = StructType(Array(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)
    ))

    val peopleDF = spark.createDataFrame(peopleRdd, structType)
    peopleDF.printSchema()
    peopleDF.show()
    peopleDF.createOrReplaceTempView("people")
    spark.sql("select * from people where age >30").show()
    spark.close()
  }

}
