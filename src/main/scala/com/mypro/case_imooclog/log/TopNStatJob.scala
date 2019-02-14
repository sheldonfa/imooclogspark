package com.mypro.case_imooclog.log

import com.mypro.case_imooclog.dao.StatDao
import com.mypro.case_imooclog.model.{DayCityVideoAccessTop3Stat, DayVideoAccessTopNStat, DayVideoTrafficsTopNStat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object TopNStatJob {

  /**
    * 按照日期统计访问量
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day:String) = {
    import spark.implicits._
    // DataFrame统计方式
    val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    // sql统计方式
    //    accessDF.createOrReplaceTempView("access_log")
    //    val sql = "select day,cmsId,count(cmsId) as times from access_log " +
    //      "where day='20161110' and cmsType='video' " +
    //      "group by day,cmsId " +
    //      "order by times desc"
    //    val videoAccessTopNDF = spark.sql(sql)

    videoAccessTopNDF.foreachPartition(record => {
      var list = new ListBuffer[DayVideoAccessTopNStat]
      record.foreach(info => {
        val day = info.getAs[String]("day")
        val cmsId = info.getAs[Long]("cmsId")
        val times = info.getAs[Long]("times")
        val stat = DayVideoAccessTopNStat(day, cmsId, times)
        list.append(stat)
      })
      StatDao.batchInsert(list)
    })
  }

  /**
    * 按照日期,地区统计每个地区流量的前三名
    */
  def cityVideoAccessTop3Stat(spark: SparkSession, accessDF: DataFrame,day:String) = {
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "city","cmsId").agg(count("city").as("times"))

    val top3 = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)).as("times_rank"))
      .filter("times_rank <=3")

    top3.foreachPartition(record => {
      var list = new ListBuffer[DayCityVideoAccessTop3Stat]
      record.foreach(info => {
        val day = info.getAs[String]("day")
        val city = info.getAs[String]("city")
        val cmsId = info.getAs[Long]("cmsId")
        val times = info.getAs[Long]("times")
        val rank = info.getAs[Int]("times_rank")
        val stat = DayCityVideoAccessTop3Stat(day, city, cmsId, times,rank)
        list.append(stat)
      })
      StatDao.batchInsertDayCityViedowAccessTop3Stat(list)
    })
  }

  def videoTrafficTopNStat(spark: SparkSession, accessDF: DataFrame,day:String) = {
    import spark.implicits._
    val df = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day","cmsId").agg(sum("traffic").as("traffics"))

    df.foreachPartition(record => {
      var list = new ListBuffer[DayVideoTrafficsTopNStat]
      record.foreach(info => {
        val day = info.getAs[String]("day")
        val cmsId = info.getAs[Long]("cmsId")
        val traffics = info.getAs[Long]("traffics")
        val stat = DayVideoTrafficsTopNStat(day, cmsId, traffics)
        list.append(stat)
      })
      StatDao.batchInsertDayViedowTrafficsTopNStat(list)
    })
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .appName("TopNStatJob").master("local[2]").getOrCreate()

    val accessDF = spark.read.parquet("C:\\home\\data\\imooclog\\cleanoutput")
    //    accessDF.printSchema()
    //    accessDF.show(false)
    val day = "20161110"
    StatDao.deleteData(day)
    // 统计imooc 每日每个课程访问量
    videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame,day)
    // 统计imooc 每日每地区top3课程访问量
    cityVideoAccessTop3Stat(spark: SparkSession, accessDF: DataFrame,day)
    // 统计imooc 每日每个课程流量
    videoTrafficTopNStat(spark:SparkSession,accessDF:DataFrame,day)

    spark.close()
  }
}
