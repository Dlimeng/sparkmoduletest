package com.lm.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
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


    val vertexRDD4:RDD[(VertexId,Int)] = sc.makeRDD(Array(
      (1L,0),
      (2L,Int.MaxValue),
      (3L,Int.MaxValue),
      (4L,Int.MaxValue),
      (5L,Int.MaxValue),
      (6L,Int.MaxValue),
      (7L,Int.MaxValue),
      (8L,Int.MaxValue),
      (9L,Int.MaxValue)
    ))
    val edgesRDD4 = sc.makeRDD(Array(
      Edge(1L,2L,6),
      Edge(1L,3L,3),
      Edge(1L,4L,1),
      Edge(3L,2L,2),
      Edge(3L,4L,2),
      Edge(2L,5L,1),
      Edge(5L,4L,6),
      Edge(5L,6L,4),
      Edge(6L,5L,10),
      Edge(5L,7L,3),
      Edge(5L,8L,6),
      Edge(4L,6L,10),
      Edge(6L,7L,2),
      Edge(7L,8L,4),
      Edge(9L,5L,2),
      Edge(9L,8L,3)
    ))

    val graph4 = Graph(vertexRDD4,edgesRDD4)
    /**
      * 第一个参数是 初始消息，面向所有节点，使用一次vprog来更新节点的值
      * 第二迭代次数
      * 第三个参数 消息发送方向  EdgeDirection.Out out代表源节点-》目标节点
      * 第一个函数vprog 更新节点程序
      * 第二函数 sendMsg 发送消息函数（主处理）
      * 第三个函数 合并
      *
      */
    val sssp = graph4.pregel(Int.MaxValue)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr!= Int.MaxValue && triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )


    println("sssp:");
    sssp.triplets
        .collect
        .foreach(triplet => println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}--edge=${triplet.attr}--dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))


  }
}
