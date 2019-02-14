package com.mypro.case_imooclog.log

import com.mypro.case_imooclog.util.AccessConvertUtil
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    var accessRDD = spark.sparkContext.textFile("C:\\home\\data\\imooclog\\output\\part-00*")
    // 过滤 url
    accessRDD = accessRDD.filter(line =>{
      var flag = true
      val splits = line.split("\t")
      val url = splits(1)
      if(url.isEmpty || url== "-") {
        flag = false
      }else{
        val domain = "http://www.imooc.com/"
        // 是http://coding.imooc.com
        if(url.indexOf(domain)<0){
          flag = false
        }else{
          val cms = url.substring(url.indexOf(domain) + domain.length, url.length)
          val cmsTypeId = cms.split("/")
          val cmsType = cmsTypeId(0)
          var cmsId = -1l
          if(cmsTypeId.size<2){
            flag = false
          }
        }
      }
      flag
    })

    val accessDF = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)), AccessConvertUtil.stuct)

//    accessDF
//    accessDF.printSchema()
//    accessDF.show(20,false)

    // coalesce(1)控制文件个数
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day").save("C:\\home\\data\\imooclog\\cleanoutput")

    spark.close()
  }
}
