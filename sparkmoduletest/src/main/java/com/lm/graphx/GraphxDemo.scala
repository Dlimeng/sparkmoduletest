package com.lm.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/**
  * @Author: limeng
  * @Date: 2019/7/8 13:25
  */
object GraphxDemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[*]")
      .appName("test1").getOrCreate()

    val context = session.sparkContext
    val myVertices: RDD[(Long, String)] = context.makeRDD(Array((1L,"Ann"),(2L,"Bill"),(3L,"Charles"),(4L,"Diane"),(5L,"Went")))
    val myEdges: RDD[Edge[String]] = context.makeRDD(Array(Edge(1L,2L,"is"),Edge(2L,3L,"is"),Edge(3L,4L,"is"),Edge(4L,5L,"like"),Edge(3L,5L,"wrote")))

    val myGraph: Graph[String, String] = Graph(myVertices,myEdges)
    //出度
    val vertices1:VertexRDD[Int] = myGraph.aggregateMessages[Int](_.sendToSrc(1),_+_)

    //使用pregel 找到最远距离
   // Pregel(myGraph.mapVertices((vid,vd) => 0),0,activeDirection = EdgeDirection.Out)

  }

  def sendMsg(ec:EdgeContext[Int,String,Int]):Unit={
    ec.sendToDst(ec.srcAttr+1)
  }

  def mergeMsg(a:Int,b:Int):Int={
    math.max(a,b)
  }
  //  def  propagateEdgeCount(g:Graph[Int,String]):Graph[Int,String]={
  //
  //  }
}
