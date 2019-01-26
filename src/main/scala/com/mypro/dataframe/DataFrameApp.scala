package com.mypro.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameApp").master("local[2]").getOrCreate()
    val peopleDf = spark.read.format("json")
      .load("file:///E:\\workspace\\study\\imooclogspark\\src\\main\\resources\\people.json")
    // 输出dataframe对应的schema信息
    peopleDf.printSchema()
    // 输出数据集的前20条记录
    peopleDf.show()
    // 查询某列所有数据：select name from table
    peopleDf.select("name").show();
    // 查询某几列的数据，并对列进行计算：select name, age+10 from table
    peopleDf.select(peopleDf.col("name"),peopleDf.col("age")+10).show();
    // 根据某一列的值进行过滤：select * from table where age>19
    peopleDf.filter(peopleDf.col("age")>19).show()
    // 根据某一列进行分组，然后再进行聚合操作：select age,count(1) from table group by age
    peopleDf.groupBy("age").count().show()

    spark.stop()
  }
}
