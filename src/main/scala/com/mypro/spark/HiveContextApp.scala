package com.mypro.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveContextApp {
  def main(args: Array[String]): Unit ={
    // 配置
    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("HiveContextApp")
    val sparkContext = new SparkContext(conf)
    val hc = new HiveContext(sparkContext)
    // 业务逻辑
    hc.table("emp").show
    // 释放资源
    sparkContext.stop()
  }
}
