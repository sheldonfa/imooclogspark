package com.mypro.case_imooclog.log

import com.mypro.case_imooclog.util.{DateTimeUtil, FormatUtil}
import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("C:\\home\\data\\imooclog\\input\\mooc_access.log")
    //    log.take(10).foreach(println)

    access.filter(line => {
      var flag = true
      // 空行不要
      val splits = line.split(" ")
      if(splits.length<3){
        flag = false
      }
      flag
    }).map(line => {
      val splits = line.split(" ")
      //      splits(0)
      // ip地址
      val ip = splits(0)
      // 时间
      if(splits.size<3){
        println(line)
        throw new RuntimeException()
      }
      val time = splits(3) + " " + splits(4)
      // url
      val url = splits(11).replaceAll("\"", "")
      // 流量
      val traffic = splits(9)
      // 返回值
      FormatUtil.format(DateTimeUtil.parse(time), url, traffic, ip)
    }).saveAsTextFile("C:\\home\\data\\imooclog\\output")

    spark.stop()

  }
}
