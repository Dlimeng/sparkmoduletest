package com.lm.graphx.algorithm

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemoBreadth
  * @Description TODO
  * @Date 2020/6/9 20:05
  * @Created by limeng
  *  广度优先遍历
  */
object GraphxDemoBreadth {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemoBreadth").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val v = sc.makeRDD(Array((1L,""),(2L,""),(3L,""),(4L,""),(5L,""),(6L,""),(7L,""),(8L,"")))

    val e =sc.makeRDD(Array(Edge(1L,2L,""),Edge(2L,3L,""),Edge(3L,4L,""),Edge(4L,1L,""),
      Edge(1L,3L,""),Edge(2L,4L,""),Edge(4L,5L,""),Edge(5L,6L,""),Edge(6L,7L,"")
      ,Edge(7L,8L,""),Edge(8L,5L,""),Edge(5L,7L,""),Edge(6L,8L,"") ))

    val graph = Graph(v,e)

    val root:VertexId = 1
    val initialGraph  = graph.mapVertices((id,_) => if(id == root) 0.0 else Double.PositiveInfinity)

    val vprog = {(id:VertexId,attr:Double,msg:Double) => math.min(attr,msg)}
    val sendMessage = {(triplet: EdgeTriplet[Double, String]) =>
      var iter:Iterator[(VertexId, Double)] = Iterator.empty
      val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
      val isDstMarked = triplet.dstAttr != Double.PositiveInfinity

      if(!(isSrcMarked && isDstMarked)){
        if(isSrcMarked){
          iter = Iterator((triplet.dstId,triplet.srcAttr + 1))
        }else{
          iter = Iterator((triplet.srcId,triplet.dstAttr + 1))
        }
      }
      iter
    }

    val reduceMessage = {(a:Double,b:Double) => math.min(a,b)}

    val bfs = initialGraph.pregel(Double.PositiveInfinity,20)(vprog,sendMessage,reduceMessage)


    println("广度优先")
    bfs.vertices.collect.foreach(println(_))
    bfs.edges.collect.foreach(println(_))

    session.stop()
  }
}
