package com.lm.spark

import org.apache.spark.sql.SparkSession

/**
  * @Classname SparkTest2
  * @Description TODO
  * @Date 2020/10/13 17:06
  * @Created by limeng
  */
object SparkTest2 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("SparkTest2").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val line = sc.makeRDD(Array("a","b","c","d","a"))

    line.map(a=> (a,1)).reduceByKey((a,b)=> a+b).collect().foreach(println(_))

    session.stop()
  }
}
