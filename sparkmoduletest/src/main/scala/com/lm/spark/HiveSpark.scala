package com.lm.spark

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Classname HiveSpark
  * @Description TODO
  * @Date 2020/4/2 18:30
  * @Created by limeng
  */
object HiveSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hive_spark").setMaster("local[*]")
    val hiveSpark=SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      //.config("spark.sql.warehouse.dir","/user/hive/warehouse")
      .getOrCreate()

    val hiveSql=hiveSpark.sqlContext
    import hiveSpark.implicits._
    import hiveSql.implicits._

    hiveSql.sql("SET hive.execution.engine=tez")
    val frame = hiveSql.sql("select * from db_test.test")

    frame.show()

    hiveSpark.stop()


  }
}
