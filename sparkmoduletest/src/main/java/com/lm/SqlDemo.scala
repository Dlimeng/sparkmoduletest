package com.lm

import org.apache.spark.sql.SparkSession

/**
  * @Classname SqlDemo
  * @Description TODO
  * @Date 2020/5/19 16:07
  * @Created by limeng
  */
object SqlDemo {
  def main(args: Array[String]): Unit = {
    val locals = "D://data"  // 输出路径，他自己创建，不能是已经存在的
    //todo:1、创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")  // 本地测试
      .config("spark.sql.warehouse.dir", locals)
      .enableHiveSupport() //开启支持hive
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别

    import spark.implicits._
    import spark.sql

    //todo:2、操作sql语句
//    sql("CREATE TABLE IF NOT EXISTS person (id int, name string, age int) row format delimited fields terminated by ' '")
//    sql("LOAD DATA LOCAL INPATH 'D://data/p.txt' INTO TABLE person")
//    sql("select * from person ").show()





    spark.stop()


  }
}
