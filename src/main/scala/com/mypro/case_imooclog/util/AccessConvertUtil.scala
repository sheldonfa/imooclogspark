package com.mypro.case_imooclog.util

import com.ggstar.util.ip.IpHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object AccessConvertUtil {

  val stuct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  def parseLog(line: String): Row = {
    val log = line.split("\t")
    val url = log(1)

    val domain = "http://www.imooc.com/"
    val cms = url.substring(url.indexOf(domain) + domain.length, url.length)
    val cmsTypeId = cms.split("/")
    if(cmsTypeId.size<2){
      println(url)
      println("==================")
      cmsTypeId.foreach(println)
      throw new RuntimeException
    }
    val cmsType = cmsTypeId(0)
    var cmsId = -1l
    if (cmsTypeId(1).matches("""\d+""")) {
      cmsId = cmsTypeId(1).toLong
    }
    val traffic = log(2).toLong
    val ip = log(3)
    val city = IpHelper.findRegionByIp(ip)
    val time = log(0).toString
    val day = time.substring(0, 10).replaceAll("-", "")

    Row(url, cmsType, cmsId, traffic, ip, city, time, day)
  }
}
