package com.lm.graphx.algorithm

import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo15
  * @Description TODO
  * @Date 2020/8/6 17:33
  * @Created by limeng
  */
object GraphxDemo15 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo15").master("local[*]").getOrCreate()
    val sc = session.sparkContext

    val root: RDD[(Long, Long)] = sc.makeRDD(Array((2L, 2L), (8L, 8L), (7L, 7L)))

    val root2: RDD[(Long, Long)] = sc.makeRDD(Array((1L, 1L)))

    val v = sc.makeRDD(Array((1L,0),(2L,0),(3L,0),(4L,0),(5L,0),(6L,0),(7L,0),(8L,0)))


    val value: RDD[Edge[Double]] = sc.makeRDD(Array(Edge(2L, 1L, 40D), Edge(3L, 1L, 20D), Edge(4L, 1L, 60D), Edge(5L, 1L, 20D),
      Edge(4L, 3L, 60D), Edge(2L, 3L, 40D), Edge(2L, 4L, 20D), Edge(6L, 4L, 40D),Edge(5L, 4L, 40D),Edge(2L, 6L, 40D),Edge(5L, 6L, 40D),Edge(7L, 6L, 20D),Edge(8L, 5L, 40D)))
    val e =value

    val graph: Graph[Int, Double] = Graph(v, e)

//    val initialGraph  = graph.reverse.outerJoinVertices(root2)((vid,vd,ud)=>{
//      val u = ud.getOrElse(Long.MaxValue)
//      if(u != Long.MaxValue){
//        GroupMeDemo15(true,List(u.toString))
//      }else{
//        GroupMeDemo15(false,List())
//      }
//    })
//    val sssp = initialGraph.pregel(GroupMeDemo15(false,Set())(
//      (id, canReach, newCanReach) => {
//
//      }, // Vertex Program
//      triplet => {  // Send Message
//        if (triplet.srcAttr && !triplet.dstAttr) {
//          Iterator((triplet.dstId, true))
//        } else {
//          Iterator.empty
//        }
//      },
//      (a, b) => a || b)
//
//
//    sssp.vertices.collect().foreach(println(_))

//    val sssp = initialGraph.pregel(GroupMeDemo15(false,List()))(gProg,gSendMsg,gMergeMsg)
//    sssp.vertices.collect().foreach(println(_))


//    val root3:VertexId = 1
//    val initialGraph  = graph.reverse.mapVertices((id,_) => if(id == root3) 0.0 else Double.PositiveInfinity)
//
//    val vprog = {(id:VertexId,attr:Double,msg:Double) => math.min(attr,msg)}
//
//    val sendMessage = {(triplet: EdgeTriplet[Double, Double]) =>
//      var iter:Iterator[(VertexId, Double)] = Iterator.empty
//
//      val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
//      val isDstMarked = triplet.dstAttr != Double.PositiveInfinity
//
//      if(!(isSrcMarked && isDstMarked)){
//        if(isSrcMarked){
//          iter = Iterator((triplet.dstId,triplet.srcAttr + 1))
//        }else{
//          iter = Iterator((triplet.srcId,triplet.dstAttr + 1))
//        }
//        println(s"sid:${triplet.srcId}-did:${triplet.dstId}")
//      }
//      iter
//    }
//
//    val reduceMessage = {(a:Double,b:Double) => math.min(a,b)}
//
//    val bfs = initialGraph.pregel(Double.PositiveInfinity,20)(vprog,sendMessage,reduceMessage)
//    println("广度优先")
//    bfs.vertices.sortBy(a=>a._2).collect().foreach(println(_))

    val srcVertex =  1L
    val bfsGraph = graph.reverse.mapVertices((id, _) => if (id == srcVertex) 0.0 else Double.PositiveInfinity)

    val unit = bfsGraph.pregel(Double.PositiveInfinity, 20, activeDirection = EdgeDirection.Either)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = edge => {
        if (edge.srcAttr != Double.PositiveInfinity) {
          Iterator((edge.dstId, edge.srcAttr + 1))
        } else {
          Iterator.empty
        }
      },
      mergeMsg = (a, b) => math.min(a, b))

    unit.vertices.collect().foreach(println(_))


  }


  def gProg(vertexId: Long,vd: GroupMeDemo15,msg: GroupMeDemo15): GroupMeDemo15 ={
    if(vd.isZou || msg.isZou){
      GroupMeDemo15(true,vd.paths ++ msg.paths)
    }else{
      GroupMeDemo15(false,vd.paths ++ msg.paths)
    }
  }

  def gSendMsg(triplet: EdgeTriplet[GroupMeDemo15, Double]): Iterator[(Long, GroupMeDemo15)] = {
    val from = triplet.srcId
    val to = triplet.dstId
    if (triplet.srcAttr.isZou && !triplet.dstAttr.isZou) {
      val paths = triplet.dstAttr.paths
      if(paths.nonEmpty){
        val tmp = paths.map(m=>{
          val last = m.split("#").last
          if(last.equals(from.toString)){
            m+"#"+to.toString
          }else{
            m+"#"+from+"#"+to
          }
        })
        Iterator((triplet.dstId, GroupMeDemo15(true,tmp)))
      }else{
        Iterator((triplet.dstId, GroupMeDemo15(true,List(to.toString))))
      }
    }else{
      Iterator.empty
    }
  }

  def gMergeMsg(ma: GroupMeDemo15, mb: GroupMeDemo15): GroupMeDemo15 = {
    if(ma.isZou || mb.isZou){
      GroupMeDemo15(true,ma.paths ++ mb.paths)
    }else{
      GroupMeDemo15(false,ma.paths ++ mb.paths)
    }
  }


}


case class GroupMeDemo15(isZou:Boolean,paths:List[String]) extends Serializable