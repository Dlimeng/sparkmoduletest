package com.lm.graphx

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo3
  * @Description TODO
  * @Date 2020/5/15 15:32
  * @Created by limeng
  *
  * 最短路径
  */
object GraphxDemo3 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[*]")
      .appName("test1").getOrCreate()
    val sc = session.sparkContext
    val graph: Graph[Long, Double] = GraphGenerators.logNormalGraph(sc,numVertices = 3).mapEdges(e=>e.attr.toDouble)

    val sourceId:VertexId = 0

    println("graph")
    println("vertices:")
    graph.vertices.collect().foreach(println(_))
    println("edges:")
    graph.edges.collect().foreach(println(_))
    println()

    val initialGraph:Graph[Double, Double] = graph.mapVertices((id, _) => if(id == sourceId) 0.0 else Double.PositiveInfinity)
    println("initialGraph:")
    println("vertices:")
    initialGraph.vertices.collect().foreach(println(_))
    println("edges:")
    initialGraph.edges.collect.foreach(println)
    /**
      * 第一个参数是 初始消息，面向所有节点，使用一次vprog来更新节点的值
      * 第二迭代次数
      * 第三个参数 消息发送方向  EdgeDirection.Out out代表源节点-》目标节点
      * 第一个函数vprog 更新节点程序
      * 第二函数 sendMsg 发送消息函数（主处理）
      * 第三个函数 合并
      *
      */
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    println();
    println("sssp:");
    println("vertices:");
    println(sssp.vertices.collect.mkString("\n"))
    println("edges:");
    sssp.edges.collect.foreach(println)


  }
}
