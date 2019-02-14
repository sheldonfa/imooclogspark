package com.mypro.case_imooclog.util

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySQLUtils {

  def getConnection: Connection = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooclog?user=root&password=root")
  }

  def close(connection: Connection, ps: PreparedStatement): Unit = {
    if (connection != null) {
      try {
        connection.close()
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
    }

    if (ps != null) {
      try {
        ps.close()
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection)
  }

}


