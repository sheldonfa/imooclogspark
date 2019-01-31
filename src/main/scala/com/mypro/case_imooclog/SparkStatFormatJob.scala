package com.mypro.case_imooclog

import java.util.Date

import com.mypro.case_imooclog.util.{DateTimeUtil, FormatUtil}
import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("C:\\home\\data\\sparkfile\\input\\10000_access.log")
    //    log.take(10).foreach(println)

    access.map(line => {
      val splits = line.split(" ")
      //      splits(0)
      // ip地址
      val ip = splits(0)
      // 时间
      val time = splits(3) + " " + splits(4)
      // url
      val url = splits(11).replace("\"", "")
      // 流量
      val traffic = splits(9)
      // 返回值
      FormatUtil.format(DateTimeUtil.parse(time), url, traffic, ip)
    }).saveAsTextFile("file:///C:\\home\\data\\sparkfile\\output" + new Date().getTime + "\\")

    spark.stop()

  }
}
