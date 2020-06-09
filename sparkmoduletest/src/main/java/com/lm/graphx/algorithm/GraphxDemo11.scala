package com.lm.graphx.algorithm

import org.apache.spark.graphx.{lib, _}
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo11
  * @Description TODO
  * @Date 2020/6/4 16:31
  * @Created by limeng
  *   社区算法标签传播
  */
object GraphxDemo11 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo10").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val v = sc.makeRDD(Array((1L,""),(2L,""),(3L,""),(4L,""),(5L,""),(6L,""),(7L,""),(8L,"")))

    val e =sc.makeRDD(Array(Edge(1L,2L,""),Edge(2L,3L,""),Edge(3L,4L,""),Edge(4L,1L,""),
      Edge(1L,3L,""),Edge(2L,4L,""),Edge(4L,5L,""),Edge(5L,6L,""),Edge(6L,7L,"")
    ,Edge(7L,8L,""),Edge(8L,5L,""),Edge(5L,7L,""),Edge(6L,8L,"") ))

    //社区算法标签传播
    lib.LabelPropagation.run(Graph(v,e),5).vertices.collect().foreach(println(_))


    session.stop()
  }
}
