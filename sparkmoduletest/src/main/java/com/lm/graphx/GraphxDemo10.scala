package com.lm.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo10
  * @Description TODO
  * @Date 2020/6/3 12:12
  * @Created by limeng
  * 连通组件
  */
object GraphxDemo10 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo10").master("local[*]").getOrCreate()

    val sc = session.sparkContext

//    val g = Graph(sc.makeRDD(1L to 7L).map((_,"")),sc.makeRDD(Array(
//      Edge(2L,5L,""),Edge(5L,3L,""),Edge(3L,2L,""),
//      Edge(4L,5L,""),Edge(6L,7L,"")
//    ))).cache()
//
//   // g.connectedComponents().vertices.collect().foreach(println(_))
//    //g.connectedComponents().edges.collect().foreach(println(_))
//    //groupByKey.map(_._2)
//    g.connectedComponents().vertices.map(_.swap).collect().foreach(println(_))


    //创建顶点数据集
    val vertexRDD:RDD[(VertexId,(String,String))] = sc.makeRDD(Array(
      (3L,("zhangsan","student")),
      (7L,("wangchen","博士后")),
      (5L,("zhangyu","教授")),
      (2L,("wangguo","教授"))
    ))
    //创建边的数据
    val edgesRDD:RDD[Edge[String]] = sc.makeRDD(Array(
      Edge(3L,7L,"合作者"),
      Edge(5L,3L,"指导"),
      Edge(2L,5L,"同事"),
      Edge(5L,7L,"同事")
    ))

    //构建一个图
    val graphx = Graph(vertexRDD,edgesRDD)

    //RDD展示
    val result = graphx.triplets.collect()

    result.foreach( triplet =>
      println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}--edge=${triplet.attr}--dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} ")
    )

    session.stop()
  }
}
