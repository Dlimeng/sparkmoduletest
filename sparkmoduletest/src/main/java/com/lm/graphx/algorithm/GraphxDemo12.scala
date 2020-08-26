package com.lm.graphx.algorithm

import com.lm.graphx.pojo.{FromInfo, InAndOut, MsgFlag, MsgToDsc, NodeOrdering}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo12
  * @Description TODO
  * @Date 2020/6/23 18:37
  * @Created by limeng
  *
  */
object GraphxDemo12 {

  def mergeMs(a:(Int,Int,VertexId),b:(Int,Int,VertexId)):(Int,Int,VertexId) = (a._1 + b._1,a._2+b._2,a._3)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo12").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val v = sc.makeRDD(Array((1L,""),(2L,""),(3L,""),(4L,""),(5L,"")))

    //Edge(5L,1L,20D)
   // val e =sc.makeRDD(Array(Edge(1L,6L,40D),Edge(2L,3L,40D),Edge(5L,2L,20D),Edge(3L,6L,40D),Edge(6L,7L,40D)))

    val e =sc.makeRDD(Array(Edge(2L,3L,40D),Edge(2L,4L,40D),Edge(3L,5L,20D),Edge(4L,5L,40D),Edge(6L,4L,40D)))
    val graph = Graph(v,e)

    /**
      * 间接
      */

    /**
      * 母节点
       */
    val roots = graph.aggregateMessages[(Int,Int,VertexId)](ctx=>{
      ctx.sendToSrc((1,0,ctx.srcId))
      ctx.sendToDst((0,1,ctx.srcId))
    },mergeMs).filter(_._2._2 == 0)

    roots.collect().foreach(println(_))
    println("roots end")


    val graph2: Graph[List[FromInfo], Double] = graph.outerJoinVertices(roots) {
      case (id, _, r) => {
        InAndOut(List(FromInfo(id, 100D, !r.isEmpty,null)), List[FromInfo]())
      }
    }.pregel(List[MsgFlag](), 10)(vprogIn, sendMsgIn, mergeMsgIn)
      .mapVertices((id, vd) => vd.in.filter(_.srcId != id))
//
    println("graph2 ")
    graph2.vertices.collect().foreach(println(_))
//
//    /**
//      * (1,List(5#20.000))
//      * (2,List(5#20.000))
//      * (3,List(1#40.000, 1#40.000, 2#45.000, 4#15.000, 5#40.000, 5#40.000, 5#45.000))
//      * (4,List())
//      * (5,List())
//      */
//   // graph2.vertices.collect().foreach(println(_))
//
//    //
//    //graph2.edges.foreach(println(_))
//
//    /**
//      * (1,List(5))
//      * (2,List(5))
//      * (3,List(1, 1, 2, 4))
//      */
//
//
//    /**
//      * (1,(List(5#20.000),List(5)))
//      * (2,(List(5#20.000),List(5)))
//      * (3,(List(1#40.000, 1#40.000, 2#45.000, 4#15.000, 5#40.000, 5#40.000, 5#45.000),List(1, 1, 2, 4)))
//      */
////    graph2.outerJoinVertices(cvs)({
////      case (_, vd, ss) => (vd, ss.getOrElse(List[Long]()))
////    }).vertices.filter(!_._2._1.isEmpty).collect().foreach(println(_))
//
//
//
//     val es = graph2.vertices.filter(f=> !f._2.isEmpty).flatMap(r => {
//          val cs = r._2.filter(f=> f.score == 101D)
//          if(!cs.isEmpty){
//            cs.map(s=> (s.srcId,r._1,s.score))
//          }else{
//            val sid = r._1
//            println(s"sid $sid")
//            val ls = r._2.groupBy(_.srcId).map(s => {
//
//              (s._1, s._2.map(_.score).sum)
//            })
//
//            val max = ls.maxBy(_._2)
//            val lsMax = ls.filter(_._2 == max._2)
//            //group scid score
//            lsMax.map(s => (s._1, r._1, s._2))
//          }
//     })

//    println("es ")
//    es.collect().foreach(println(_))
//
//    val ep = es.map(m=>(m._2,(List((m._1, m._3))))).reduceByKey((a,b)=>{
//      val max = a.union(b).maxBy(_._2)._2
//      a.union(b).filter(_._2 == max)
//    }).flatMap(f=>{
//      f._2.map(s=>Edge(s._1,f._1,s._2))
//    })
//    println("ep ")
//    ep.collect().foreach(println(_))
//
//
//    val tmpEdges = Graph.fromEdges(ep, 0).mapVertices((id,_)=>{
//      (id)
//    }).outerJoinVertices(graph.vertices)({
//      case (_, _, d) => d
//    })
//
//    println("tmpEdges ")
//    tmpEdges.vertices.collect().foreach(println(_))
//    tmpEdges.edges.collect().foreach(println(_))

   // es.collect().foreach(println(_))

//   graph2.outerJoinVertices(cvs)({
//      case (_, vd, ss) => (vd, ss.getOrElse(List[Long]()))
//    }).vertices.filter(!_._2._1.isEmpty)
//      .flatMap(r => {
//        val ls = r._2._1.groupBy(_.srcId).map(s => {
//          (s._1, s._2.map(_.score).sum)
//        })
//        val max = ls.maxBy(_._2)
//        val lsMax = ls.filter(_._2 == max._2)
//        lsMax.map(s => (s._1, r._1, s._2))
//      })
////
    /**
      * (5,1,20.0)
      * (5,2,20.0)
      * (5,3,125.0)
      */
   // es.collect().foreach(println(_))





    /**
      * 最大股东
      * (2,List(MsgToDsc(5,20.0)))
      * (3,List(MsgToDsc(1,40.0), MsgToDsc(2,45.0), MsgToDsc(4,15.0)))
      */
//    graph.aggregateMessages[List[MsgToDsc]](ctx => {
//      ctx.sendToDst(List(MsgToDsc(ctx.srcId, ctx.attr)))
//    }, mergeDsc).collect().foreach(println(_))

//    println("最大股东")
//    val value: RDD[Edge[Double]] = graph2.aggregateMessages[List[MsgToDsc]](ctx => {
//      ctx.sendToDst(List(MsgToDsc(ctx.srcId, ctx.attr)))
//    }, mergeDsc).map(m => {
//      val max = m._2.max(NodeOrdering)
//      Edge(max.srcId, m._1, max.score)
//    })


//    value.collect().foreach(println(_))





    session.stop()


  }


  def mergeDsc(a: List[MsgToDsc], b: List[MsgToDsc]): List[MsgToDsc] = a ++ b


  def vprogIn(vertexId: Long,vd:InAndOut,news:List[MsgFlag]):InAndOut ={
    if (news == null || news.isEmpty) vd else {
      val in = vd.in ++ news.filter(_.flag == 0).map(r => FromInfo(r.srcId, r.score, r.root,r.path))
      if (vd.out == null) {
        val out = news.filter(_.flag == 1).map(r => FromInfo(r.srcId, r.score, r.root,r.path))
        InAndOut(in, out)
      } else {
        val out = vd.out ++ news.filter(_.flag == 1).map(r => FromInfo(r.srcId, r.score, r.root,r.path))
        InAndOut(in, out)
      }
    }
  }


  def sendMsgIn(triplet: EdgeTriplet[InAndOut, Double]): Iterator[(VertexId, List[MsgFlag])] = {

    var tm = triplet.srcAttr.in diff (triplet.srcAttr.out)
    //恶心的环
    if (!tm.isEmpty && tm.map(_.srcId).contains(triplet.dstId)) tm = tm.diff(triplet.dstAttr.in)

    if (!tm.isEmpty) {
      val toIn = tm.map(r => {
        val s = r.score * triplet.attr / 100D
        val p = r.path
        if(p == null) MsgFlag(r.srcId, s, 0, r.root,r.srcId.toString)
        else MsgFlag(r.srcId, s, 0, r.root,p+"-"+triplet.srcId)
      })
      val toOut = tm.map(r => MsgFlag(r.srcId, r.score, 1, r.root,r.path))
      Iterator((triplet.dstId, toIn), (triplet.srcId, toOut))
    } else Iterator.empty
  }

  def mergeMsgIn(a: List[MsgFlag], b: List[MsgFlag]): List[MsgFlag] = a ++ b

}
