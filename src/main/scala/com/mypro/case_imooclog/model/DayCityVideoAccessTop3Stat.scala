package com.mypro.case_imooclog.model

/**
  * 根据日期统计不同课程id的访问量
  */
case class DayCityVideoAccessTop3Stat(day: String, city:String,cmsId: Long,times: Long,rank:Int)