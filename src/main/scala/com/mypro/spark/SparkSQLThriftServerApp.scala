package com.mypro.spark

import java.sql.DriverManager

object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://hadoop001:10000", "hadoop", "fx153542")
    val pstmt = conn.prepareStatement("select * from emp")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      //empno  |    ename    |    job    |  mgr  |  hiredate  |    sal    |  comm  | deptno
      println(rs.getString("empno") + "  "
        + rs.getString("ename") + "  "
        + rs.getString("job") + "  "
        + rs.getString("mgr") + "  "
        + rs.getString("hiredate") + "  "
        + rs.getString("sal") + "  "
        + rs.getString("comm") + "  "
        + rs.getString("deptno")
      )
    }
    rs.close()
    pstmt.close()
    conn.close()
  }
}
