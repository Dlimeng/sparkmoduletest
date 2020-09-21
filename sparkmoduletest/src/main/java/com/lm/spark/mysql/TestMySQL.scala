package com.lm.spark.mysql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Classname TestMySQL
  * @Description TODO
  * @Date 2020/9/18 15:22
  * @Created by limeng
  */
object TestMySQL {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo10").master("local[*]").getOrCreate()

    val sc = session.sparkContext
    val sQLContext = session.sqlContext

    val properties = new Properties()
    properties.put("user", "dt")
    properties.put("password","dt")
    val url = "jdbc:mysql://mysql.knowlegene.com:3306/dt"
    val tableName = "removecompanynode"

    val df: DataFrame = sQLContext.read.jdbc(url,tableName,properties)


    df.select("entid").rdd.map(m => {
      m.getString(0)
    }).zipWithUniqueId().collect().foreach(println(_))



    session.stop()
  }
}
