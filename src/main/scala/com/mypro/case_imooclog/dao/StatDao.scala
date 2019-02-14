package com.mypro.case_imooclog.dao

import java.sql.{Connection, PreparedStatement}

import com.mypro.case_imooclog.model.{DayCityVideoAccessTop3Stat, DayVideoAccessTopNStat, DayVideoTrafficsTopNStat}
import com.mypro.case_imooclog.util.MySQLUtils

import scala.collection.mutable.ListBuffer

object StatDao {
  def deleteData(day: String) = {

    val tables = Array("day_city_video_access_top3_stat","day_video_access_top_stat","day_video_traffics_topn_stat")

    var conn:Connection = null
    var pstmt:PreparedStatement = null
    try{
      conn = MySQLUtils.getConnection
      for(t<-tables){
        pstmt = conn.prepareStatement(s"delete from $t where day = ?")
        pstmt.setString(1,day)
        pstmt.executeUpdate()
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      MySQLUtils.close(conn,pstmt)
    }

  }

  def batchInsert(list: ListBuffer[DayVideoAccessTopNStat]): Unit = {
    val conn = MySQLUtils.getConnection
    conn.setAutoCommit(false)
    var pstmt = conn.prepareStatement("insert into day_video_access_top_stat(day,cms_id,times) values(?,?,?)")
    try {
      for(row <-list){
        pstmt.setString(1, row.day)
        pstmt.setLong(2, row.cmsId)
        pstmt.setLong(3, row.times)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.close(conn, pstmt)
    }
  }


  def batchInsertDayCityViedowAccessTop3Stat(list: ListBuffer[DayCityVideoAccessTop3Stat]): Unit = {
    val conn = MySQLUtils.getConnection
    conn.setAutoCommit(false)
    var pstmt = conn.prepareStatement("insert into day_city_video_access_top3_stat(day,city,cms_id,times,rank) values(?,?,?,?,?)")
    try {
      for(e <-list){
        pstmt.setString(1, e.day)
        pstmt.setString(2, e.city)
        pstmt.setLong(3, e.cmsId)
        pstmt.setLong(4, e.times)
        pstmt.setLong(5, e.rank)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.close(conn, pstmt)
    }
  }

  def batchInsertDayViedowTrafficsTopNStat(list: ListBuffer[DayVideoTrafficsTopNStat]): Unit = {
    val conn = MySQLUtils.getConnection
    conn.setAutoCommit(false)
    var pstmt = conn.prepareStatement("insert into day_video_traffics_topn_stat(day,cms_id,traffics) values(?,?,?)")
    try {
      for(e <-list){
        pstmt.setString(1, e.day)
        pstmt.setLong(2, e.cmsId)
        pstmt.setLong(3, e.traffics)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.close(conn, pstmt)
    }
  }
}
