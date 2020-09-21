package com.lm.graphx.algorithm

import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo19
  * @Description TODO
  * @Date 2020/9/10 16:26
  * @Created by limeng
  */
object GraphxDemo19 {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("GraphxDemo18")
      .master("local[*]")
      .getOrCreate()


    val sc = session.sparkContext

    val list = sc.makeRDD(Array(1L,2L,2L,3L,4L))
    val tmp = list.map(m=>(m,(System.currentTimeMillis(),List(m))))

    tmp.collect().foreach(println(_))


    sc.stop()

  }

}
