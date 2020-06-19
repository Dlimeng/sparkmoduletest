package com.lm.graphx.algorithm

import com.lm.graphx.pojo.{GroupVD, MsgScore}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemoInOut2
  * @Description TODO
  * @Date 2020/6/16 20:32
  * @Created by limeng
  * 新版本划分成员
  */
object GraphxDemoInOut2 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemoBreadth").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val edgeVal = sc.makeRDD(Array(Edge(1L,2L,100d),Edge(1L,3L,40d),Edge(2L,7L,100d),Edge(2L,6L,20d),Edge(7L,6L,30d),Edge(3L,6L,50d)))

    val graph = Graph.fromEdges(edgeVal,1)

    val tupleRoot: RDD[(Long, Long)] = sc.makeRDD(Array((3L,3L)))

    val value: Graph[GroupVD, Double] = graph.outerJoinVertices(tupleRoot)((vid, vd, ud) => {
      val u = ud.getOrElse(Long.MaxValue)

      if (u != Long.MaxValue) {
        GroupVD(Set(MsgScore(u, u, u, 100D)), Set[MsgScore](), Set(vid), true)

       // GroupVD(Set[MsgScore](), Set[MsgScore](), Set(), true)
      } else {
        GroupVD(Set[MsgScore](), Set[MsgScore](), Set(), false)
      }
    }).pregel(GroupVD(Set[MsgScore](), Set[MsgScore](), Set[Long](), false), 10)(gProg, gSendMsg, gMergeMsg)

    val graphValue = value


    graphValue.vertices.collect().foreach(println(_))


    session.stop()
  }

  def gSendMsg(triplet: EdgeTriplet[GroupVD, Double]):  Iterator[(Long, GroupVD)]  ={
    val from = triplet.srcId
    val to = triplet.dstId

    val unsent = triplet.srcAttr.accept.diff(triplet.srcAttr.sent)
    if(unsent.size != 0){
      val controlKey = triplet.srcAttr.accept.groupBy(u => u.groupId).map(m=>{
        m._1 -> m._2.map(_.score).sum
      }).filter(m=>{
        if(triplet.srcAttr.isListed == true) {
          m._2 > 10
        }else {
          m._2 > 50
        }
      })

      val toSendMsg: Set[MsgScore] = unsent.filter(f => {
        controlKey.contains(f.groupId)
      })


      if(toSendMsg.size != 0 && !triplet.srcAttr.ids.contains(triplet.dstId)){
        val msg = toSendMsg.map(m=>{
          MsgScore(m.groupId,from,to,triplet.attr)
        })

        Iterator((triplet.dstId,GroupVD(msg,Set[MsgScore](),triplet.srcAttr.ids ++ Set(triplet.dstId),triplet.dstAttr.isListed)),
          (triplet.srcId,GroupVD(Set[MsgScore](),msg,triplet.srcAttr.ids,triplet.srcAttr.isListed)))
      }
    }
    Iterator.empty
  }

  def gProg(vertexId:Long,vd: GroupVD,msg: GroupVD) ={
    val accept = vd.accept ++ msg.accept.groupBy(a=> a.groupId).flatMap(f=>f._2)
    println("gProg start")
    accept.toList.foreach(f=>println(f.toString))
    println("gProg end")
    GroupVD(accept,vd.sent ++ msg.sent,msg.ids ++ vd.ids,vd.isListed)
  }
  def gMergeMsg(ma:GroupVD,mb:GroupVD) ={
    GroupVD(ma.accept ++ mb.accept,ma.sent++mb.sent,ma.ids ++ mb.ids,ma.isListed)
  }
}
