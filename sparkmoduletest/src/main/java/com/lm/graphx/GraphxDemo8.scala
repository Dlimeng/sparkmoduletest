package com.lm.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo8
  * @Description TODO
  * @Date 2020/5/28 19:48
  * @Created by limeng
  */
object GraphxDemo8 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo8").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val edge1: RDD[Edge[Int]] = sc.makeRDD(Array(
      Edge(2, 1, 0),
      Edge(4, 1, 0),
      Edge(1, 2, 0),
      Edge(6, 3, 0),
      Edge(7, 3, 0),
      Edge(7, 6, 0),
      Edge(6, 7, 0),
      Edge(3, 7, 0)
    ))

    val vertexValue: RDD[(VertexId, String)] = sc.makeRDD(Array(
      (1L, "ba1"),
      (2L, "ba2"),
      (3L, "ba3"),
      (4L, "ba4"),
      (6L, "ba6"),
      (7L, "ba7"),
      (8L, "ba8")
    ))

    val graph1 = Graph(vertexValue,edge1)

    //查找顶点
    val graphVertex: VertexRDD[(Int, Int, String)] = graph1.mapVertices((vid, vd) => (0, 0, vd))
      .aggregateMessages[(Int,Int, String)](ctx => {
        ctx.sendToSrc(0, 1, ctx.srcAttr._3)
        ctx.sendToDst(1, 0, ctx.dstAttr._3)
      }, (a, b) => (a._1 + b._1, a._2 + b._2, a._3))
      .filter(_._2._1 == 0)


    graphVertex.collect().foreach(println(_))

  }
}
