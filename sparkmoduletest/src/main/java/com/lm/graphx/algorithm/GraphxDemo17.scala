package com.lm.graphx.algorithm

import com.lm.graphx.algorithm.GraphxDemo16.{gMergeMsg, gProg, gSendMsg, mergeMs}
import com.lm.spark.model.{GroupMem, MemRel}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo17
  * @Description TODO
  * @Date 2020/9/2 15:30
  * @Created by limeng
  */
object GraphxDemo17 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo17").master("local[*]").getOrCreate()
    val sc = session.sparkContext



    val v = sc.makeRDD(Array((1L,0),(2L,0),(3L,0),(4L,0),(5L,0),(6L,0),(7L,0),(8L,0),(9L,0),(10L,0)))

    val e = sc.makeRDD(Array(Edge(1L, 2L, 51D), Edge(1L, 4L, 30D), Edge(6L, 7L, 51D), Edge(6L, 8L, 51D),
      Edge(2L, 4L, 30D), Edge(4L, 5L, 30D), Edge(7L, 4L, 30D), Edge(7L, 5L, 30D), Edge(9L, 10L, 20D),Edge(7L, 1L, 20D)))


    val graph: Graph[Int, Double] = Graph(v, e)

    val roots = sc.makeRDD(Array(1L,6L))
//
//    val roots = graph.aggregateMessages[(Int,Int,VertexId)](ctx=>{
//      ctx.sendToSrc((1,0,ctx.srcId))
//      ctx.sendToDst((0,1,ctx.srcId))
//    },mergeMs)

    val head = roots.map(m=> (m,m))




    val queryGroup = graph.outerJoinVertices(head)((vid,vd,ud) =>{
      val u = ud.getOrElse(Long.MaxValue)
      if(u != Long.MaxValue){
        GroupVD(Set(MsgScore(u,u,u,100D)),Set[MsgScore](),Set(vid))
      }else{
        GroupVD(Set[MsgScore](), Set[MsgScore](), Set())
      }
    }).pregel(GroupVD(Set[MsgScore](), Set[MsgScore](), Set[Long]()),
      10)(gProg, gSendMsg, gMergeMsg)


    val mm = queryGroup.vertices.flatMap(f=> {
      f._2.accept.groupBy(_.groupId).map(m=>{
        val rel = m._2.map(msg => {
          MemRel(msg.from, msg.to, msg.score, 1)
        })
        GroupMem(f._1, m._1, m._2.map(_.score).sum, 1, 0, rel)
      }).filter(f=> f.score > 50D)
    })

    println("mm成员")
    mm.collect().foreach(println(_))


    val graph2 = graph.edges.filter(l=> l.srcId != l.dstId).map(m=> (m.dstId,(m.srcId,m.dstId)))


    head.join(graph2)



  }



}
