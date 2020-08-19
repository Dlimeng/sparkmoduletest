package com.lm.graphx.algorithm

import com.lm.graphx.algorithm.GraphxDemo12.mergeMs
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

//    val v = sc.makeRDD(Array((1L,""),(2L,""),(3L,""),(4L,""),(5L,""),(6L,""),(7L,""),(8L,"")))
//
//    val e =sc.makeRDD(Array(Edge(1L,2L,""),Edge(2L,3L,""),Edge(3L,4L,""),Edge(4L,1L,""),
//      Edge(1L,3L,""),Edge(2L,4L,""),Edge(4L,5L,""),Edge(5L,6L,""),Edge(6L,7L,"")
//      ,Edge(7L,8L,""),Edge(8L,5L,""),Edge(5L,7L,""),Edge(6L,8L,"") ))

//    val v = sc.makeRDD(Array((1L,""),(2L,""),(3L,""),(5L,"")))
//
//     val e =sc.makeRDD(Array(Edge(1L,3L,40D),Edge(2L,3L,45D),Edge(5L,2L,20D),Edge(5L,1L,20D)))

    val v = sc.makeRDD(Array((1L,"A"),(2L,"B"),(3L,"C"),(4L,"D"),(5L,"E"),(6L,"F"),(7L,"G")))

    val e =sc.makeRDD(Array(Edge(1L,2L,7.0),Edge(1L,4L,5.0),Edge(2L,3L,8.0),Edge(2L,4L,9.0),
      Edge(2L,5L,7.0),Edge(3L,5L,5.0),Edge(4L,5L,15.0),Edge(4L,6L,6.0),Edge(5L,6L,8.0)
      ,Edge(5L,7L,9.0),Edge(6L,7L,11.0)))

    val graph = Graph(v,e)

    /**
      * 母节点
      */
//    val roots = graph.aggregateMessages[(Int,Int)](ctx=>{
//      ctx.sendToSrc((1,0))
//      ctx.sendToDst((0,1))
//    },mergeMs).filter(_._2._2 == 0)


    val root:VertexId = 1
    val initialGraph  = graph.mapVertices((id,_) => if(id == root) 0.0 else Double.PositiveInfinity)

    val vprog = {(id:VertexId,attr:Double,msg:Double) => math.min(attr,msg)}

    val sendMessage = {(triplet: EdgeTriplet[Double, Double]) =>
      var iter:Iterator[(VertexId, Double)] = Iterator.empty

      val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
      val isDstMarked = triplet.dstAttr != Double.PositiveInfinity

      if(!(isSrcMarked && isDstMarked)){
        if(isSrcMarked){
          iter = Iterator((triplet.dstId,triplet.srcAttr + 1))
        }else{
          iter = Iterator((triplet.srcId,triplet.dstAttr + 1))
        }
        println(s"sid:${triplet.srcId}-did:${triplet.dstId}")
      }
      iter
    }

    val reduceMessage = {(a:Double,b:Double) => math.min(a,b)}

    val bfs = initialGraph.pregel(Double.PositiveInfinity,20)(vprog,sendMessage,reduceMessage)


    /**
      * (1,0.0)
      * (2,1.0)
      * (4,1.0)
      * (3,2.0)
      * (5,2.0)
      * (6,2.0)
      * (7,3.0)
      */
    println("广度优先")
    bfs.vertices.sortBy(a=>a._2).collect().foreach(println(_))
   // bfs.edges.collect.foreach(println(_))

    session.stop()
  }
}
