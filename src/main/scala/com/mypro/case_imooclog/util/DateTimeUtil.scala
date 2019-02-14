package com.mypro.case_imooclog.util

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateTimeUtil {
  // 10/Nov/2016:00:01:02 +0800
  val YYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 获取输入时间：long类型
    *
    * @param time
    */
  def getTime(time: String): Long = {
    try {
      YYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]")))
        .getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  /**
    * 获取时间：yyyy-MM-dd HH:mm:ss
    *
    * @param time
    */
  def parse(time: String): String = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }


  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 +0800])"))
  }
}
