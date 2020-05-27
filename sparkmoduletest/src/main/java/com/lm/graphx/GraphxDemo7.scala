package com.lm.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo7
  * @Description TODO
  * @Date 2020/5/27 15:46
  * @Created by limeng
  */
object GraphxDemo7 {
  def main(args: Array[String]): Unit = {
     val session = SparkSession.builder().appName("GraphxDemo7").master("local[*]").getOrCreate()

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



    val graph1: Graph[Int, Int] = Graph.fromEdges(edge1,0)

    println("graph1:")
    graph1.edges.collect().foreach(println(_))
    graph1.vertices.collect().foreach(println(_))

    println("pageRank:")
    /**
      * pageRank 动态算法
      * 参数值越小得到的结果越有说服力
      */

    val graph2: Graph[Double, Double] = graph1.pageRank(0.0001)
    graph2.edges.collect().foreach(println(_))
    graph2.vertices.collect().foreach(println(_))
    println("pageRank join:")
    graph2.vertices.join(vertexValue).collect().foreach(println(_))

    /**
      * 1.pagerank 当一个顶点只有入度没有出度时将不断吞噬掉该有向图其他顶点的PR值，最终使得所有顶点pr值都变成0
      * 2.带入参数不同造成结果不同，例如静态和动态调用哪种更适合，又或者迭代次数的选择、前后两次迭代的差值限定又该选择多少，这些都是没有固定标准的。
      * 另外，从测试结果可以看出目前静态调用方式（即设置固定）
      *
      */




  }
}
