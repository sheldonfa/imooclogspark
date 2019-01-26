package com.mypro.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SqlContextApp {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\develops\\winutils")

    val path = args(0)

    // 定义配置文件
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("aaa")
    // 声明sqlContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    // 业务逻辑
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()
  }
}
