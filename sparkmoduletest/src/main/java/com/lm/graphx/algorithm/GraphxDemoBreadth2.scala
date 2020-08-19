package com.lm.graphx.algorithm

import com.lm.graphx.algorithm.GraphxDemo14.{gMergeMsg, gProg, gSendMsg}
import com.lm.spark.model.{GroupMem, MemRel}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemoBreadth2
  * @Description TODO
  * @Date 2020/8/4 14:57
  * @Created by limeng
  * 层级遍历
  */
object GraphxDemoBreadth2 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemoBreadth2").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val v = sc.makeRDD(Array((1L,"A"),(2L,"B"),(3L,"C"),(4L,"D"),(5L,"E"),(6L,"F"),(7L,"G")))

    val e =sc.makeRDD(Array(Edge(1L,2L,7.0),Edge(1L,4L,5.0),Edge(2L,3L,8.0),Edge(2L,4L,9.0),
      Edge(2L,5L,7.0),Edge(3L,5L,5.0),Edge(4L,5L,15.0),Edge(4L,6L,6.0),Edge(5L,6L,8.0)
      ,Edge(5L,7L,9.0),Edge(6L,7L,11.0)))


    val root: RDD[(Long, Long)] = sc.makeRDD(Array((1L, 1L)))

    val graph: Graph[String, Double] = Graph(v, e)

    val quaGroup = graph.outerJoinVertices(root)((vid,vd,ud)=>{
      val u = ud.getOrElse(Long.MaxValue)
      if(u!=Long.MaxValue){
        BVD(Set(BScore(u, u, u, 100D)), Set[BScore](), Set(vid))
      }else{
        BVD(Set[BScore](), Set[BScore](), Set())
      }
    }).pregel(BVD(Set[BScore](), Set[BScore](), Set[Long]()),
      10)(gProg, gSendMsg, gMergeMsg)

    val mm:RDD[GroupMem] = quaGroup.vertices.flatMap(f=>{
      f._2.accept.groupBy(_.groupId).map(m=>{
        val rel = m._2.map(msg => {
          MemRel(msg.from, msg.to, msg.score, 1)
        })
        GroupMem(f._1, m._1, m._2.map(_.score).sum, 1, 0, rel)
      })
    })

    mm.collect().foreach(println(_))





  }

  def gProg(vertexId: Long,vd: BVD,msg: BVD): BVD ={
    val accept = vd.accept ++ msg.accept.groupBy(a=>a.groupId).flatMap(f=> f._2)

    BVD(accept,vd.sent ++ msg.sent,msg.ids++vd.ids)
  }
  def gSendMsg(triplet: EdgeTriplet[BVD, Double]): Iterator[(Long, BVD)] = {
    val from = triplet.srcId
    val to = triplet.dstId
    val unsent = triplet.srcAttr.accept.diff(triplet.srcAttr.sent)

    if(unsent.nonEmpty){
      //val contrlKey = triplet.srcAttr.accept.groupBy(u=>u.groupId).map(m=>{m._1 -> m._2.map(_.score).sum})
      if(!triplet.srcAttr.ids.contains(triplet.dstId)){

        val msg = unsent.map(a => {
          BScore(a.groupId, from, to, triplet.attr)
        })

        Iterator((triplet.dstId, BVD(msg, Set[BScore](), triplet.srcAttr.ids ++ Set(triplet.dstId))),
          (triplet.srcId, BVD(Set[BScore](), msg, triplet.srcAttr.ids)))
      }else{
        Iterator.empty
      }
    }else{
      Iterator.empty
    }
  }


  def gMergeMsg(ma: BVD, mb: BVD): BVD = {
    BVD(ma.accept ++ mb.accept, ma.sent ++ mb.sent, ma.ids ++ mb.ids)
  }

  case class BScore(groupId: Long, from: Long, to: Long, score: Double) extends Serializable

  case class BVD(accept: Set[BScore], sent: Set[BScore], ids: Set[Long]) extends Serializable

}
