package com.mypro.case_imooclog.util

object FormatUtil {
  def format(args: String*): String = {
    var result = ""
    val split = "\t"
    for (i <- 0 until (args.size)) {
      result = result + args(i)
      if (i < args.size - 1) {
        result = result + split
      }
    }
    result
  }

  def main(args: Array[String]): Unit = {
    println(format("Kitty", "Tom", "Luke", "Kit"))
  }
}
