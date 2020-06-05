package com.lm.spark.shuffle

import org.apache.spark.sql.SparkSession

/**
  * @Classname SparkTest2
  * @Description TODO
  * @Date 2020/6/5 10:26
  * @Created by limeng
  * 采样倾斜key并分拆join操作
  */
object SparkTest2 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo10").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val value = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))), 3)

    value.sample(false,0.1).collect().foreach(println(_))




    session.stop()
  }
}
