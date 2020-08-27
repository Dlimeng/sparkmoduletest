package com.lm.graphx.algorithm


import com.lm.graphx.pojo.{FromInfo, InAndOut, MsgFlag}
import com.lm.spark.model.{GroupMem, MemRel}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @Classname GraphxDemo16
  * @Description TODO
  * @Date 2020/8/27 14:03
  * @Created by limeng
  */
object GraphxDemo16 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo16").master("local[*]").getOrCreate()
    val sc = session.sparkContext

    val v = sc.makeRDD(Array((1L,0),(2L,0),(3L,0),(4L,0),(5L,0),(6L,0),(7L,0),(8L,0)))


    val e = sc.makeRDD(Array(Edge(1L, 2L, 51D), Edge(1L, 4L, 30D), Edge(6L, 7L, 51D), Edge(6L, 8L, 51D),
      Edge(2L, 4L, 30D), Edge(4L, 5L, 30D), Edge(7L, 4L, 30D), Edge(7L, 5L, 30D)))

    val graph: Graph[Int, Double] = Graph(v, e)

    val roots = graph.aggregateMessages[(Int,Int,VertexId)](ctx=>{
      ctx.sendToSrc((1,0,ctx.srcId))
      ctx.sendToDst((0,1,ctx.srcId))
    },mergeMs)

    val head = roots.filter(f=> f._2._2 == 0).map(m=>(m._2._3.toLong,m._2._3.toLong))
    val tail = roots.filter(f=> f._2._1 == 0).map(m=>(m._2._3.toLong,m._2._3.toLong))

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

    println("成员：")
    mm.collect().foreach(println(_))

    val mmrel = mm.groupBy(_.groupId).flatMap(f=>{
      f._2.flatMap(f2=>{
        f2.rel.map(m=> (m.from,m.to,m.score,f._1))
      })
    }).map(m=>(m._1+"#"+m._2,MVD(m._1,m._2,m._3,Set(m._4)))).reduceByKey((a,b)=>{
      MVD(a.from,a.to,a.score,a.ids++b.ids)
    })

    println("成员关系：")
    mmrel.collect().foreach(println(_))

    val allrel = graph.edges.filter(f=> f.attr < 50D && f.attr > 5D ).map(m=> (m.srcId+"#"+m.dstId,(m.srcId,m.dstId,m.attr)))


    val intersection = allrel.join(mmrel).map(_._2._1)

    println("相对关系")
    val subrel = allrel.map(_._2).subtract(intersection)
    subrel.collect().foreach(println(_))

    val subEdge = subrel.map(m=>Edge(m._1,m._2,m._3))

    val subGraph = Graph.fromEdges(subEdge,1L)

    val subRoots = subGraph.aggregateMessages[(Int,Int,VertexId)](ctx=>{
      ctx.sendToSrc((1,0,ctx.srcId))
      ctx.sendToDst((0,1,ctx.srcId))
    },mergeMs)

    val headSub = subRoots.filter(f=> f._2._2 == 0).map(m=>(m._2._3.toLong,m._2._3.toLong))
    val tailSub = subRoots.filter(f=> f._2._1 == 0).map(m=>(m._2._3.toLong,m._2._3.toLong))


    val subGraphs = subGraph.outerJoinVertices(headSub)((vid,vd,ud)=>{
      val u = ud.getOrElse(Long.MaxValue)
      if(u != Long.MaxValue){
        InAndOut(List(FromInfo(vid, 100D, true,null)), List[FromInfo]())
      }else{
        InAndOut(List(FromInfo(vid, 100D, false,null)), List[FromInfo]())
      }
    }).pregel(List[MsgFlag](), 10)(vprogIn, sendMsgIn, mergeMsgIn)
      .mapVertices((id, vd) => vd.in.filter(_.srcId != id))

    println("FromInfos ")

    val subVertices =  subGraphs.vertices.filter(f=> f._2.nonEmpty).map(m=> (m._1,m._2.filter(_.root).sortBy(_.path.split("#").length).reverse))
    subVertices.collect().foreach(println(_))

    val bSub  = sc.broadcast(subVertices.collect())
    val bMrel = sc.broadcast(mm.collect())



    tailSub.map(m=>{
      val tailRoot =  m._1
      val sublist = bSub.value.filter(f=> f._1 == tailRoot).head._2
      //散点子图中
      val hashmap = new mutable.HashMap[String,Long]()
      if(sublist.nonEmpty){
        sublist.map(p=>{
          p.path.split("#")


        })
      }


    })



    session.stop()
  }

  //合并
  def mergeMs(a:(Int,Int,VertexId),b:(Int,Int,VertexId)):(Int,Int,VertexId) = (a._1 + b._1,a._2+b._2,a._3)


  def gProg(vertexId: Long, vd: GroupVD, msg: GroupVD) = {
    val accept = vd.accept ++ msg.accept.groupBy(_.groupId).flatMap(b=> b._2)
    GroupVD(accept,vd.sent ++ msg.sent,msg.ids)
  }

  def gSendMsg(triplet: EdgeTriplet[GroupVD, Double]): Iterator[(Long, GroupVD)] = {
    val from = triplet.srcId
    val to = triplet.dstId
    val unsent = triplet.srcAttr.accept.diff(triplet.srcAttr.sent)

    //获取投资占股50%
    if(unsent.nonEmpty){
      val controlKey = triplet.srcAttr.accept.groupBy(_.groupId).map(m=>{
        m._1 -> m._2.map(_.score).sum
      }).filter(f=> f._2 > 50D)

      val toSendMsg = unsent.filter(f=>{
        controlKey.contains(f.groupId)
      })


      if(toSendMsg.nonEmpty && !triplet.srcAttr.ids.contains(triplet.dstId)){
        val msg = toSendMsg.map(a=> MsgScore(a.groupId,from,to,triplet.attr))

        Iterator((triplet.dstId, GroupVD(msg, Set[MsgScore](), triplet.srcAttr.ids ++ Set(triplet.dstId))),
          (triplet.srcId, GroupVD(Set[MsgScore](), msg, triplet.srcAttr.ids)))
      }else{
        Iterator.empty
      }

    }else{
      Iterator.empty
    }
  }
  def gMergeMsg(ma: GroupVD, mb: GroupVD): GroupVD = {
    GroupVD(ma.accept ++ mb.accept, ma.sent ++ mb.sent, ma.ids ++ mb.ids)
  }
  case class MVD(from: Long, to: Long, score: Double, ids: Set[Long])  extends Serializable
  case class InAndOut(in:List[FromInfo],out:List[FromInfo]) extends Serializable

  // flag 1 给OUT  0 给In
  case class MsgFlag(srcId: Long, score: Double, flag: Int, root: Boolean,path:String) extends Serializable {
    override def toString: String = srcId + " # " + flag
  }

  case class FromInfo(srcId:Long,score:Double, root: Boolean,path:String) extends Serializable{
    override def hashCode(): Int = srcId.hashCode()

    override def equals(obj: Any): Boolean = {
      if(obj == null) false else{
        val o = obj.asInstanceOf[FromInfo]
        o.srcId.equals(this.srcId)
      }
    }

    override def toString: String = srcId+"#"+ root.toString+"#"+path +"#"+score.formatted("%.3f")
  }

  def vprogIn(vertexId: Long,vd:InAndOut,news:List[MsgFlag]):InAndOut ={
    if(news == null || news.isEmpty) vd else{
      val in = vd.in ++ news.filter(_.flag == 0).map(r=> FromInfo(r.srcId,r.score,r.root,r.path))
      if(vd.out == null){
        val out = news.filter(_.flag == 1).map(r=> FromInfo(r.srcId,r.score,r.root,r.path))
        InAndOut(in,out)
      }else{
        val out = vd.out ++ news.filter(_.flag == 1).map(r => FromInfo(r.srcId, r.score, r.root,r.path))
        InAndOut(in, out)
      }
    }
  }

  def sendMsgIn(triplet: EdgeTriplet[InAndOut, Double]): Iterator[(VertexId, List[MsgFlag])] = {
    var tm = triplet.srcAttr.in.diff(triplet.srcAttr.out)
    if(tm.nonEmpty && tm.map(_.srcId).contains(triplet.dstId)){
      tm = tm.diff(triplet.dstAttr.in)
    }

    if(tm.nonEmpty){
     val toIn = tm.map(r=> {
        val s = r.score * triplet.attr / 100D
        val p = r.path
        if(p == null) MsgFlag(r.srcId, s, 0, r.root,r.srcId.toString)
        else MsgFlag(r.srcId, s, 0, r.root,p+"#"+triplet.srcId)
      })
     val toOut = tm.map(r => MsgFlag(r.srcId, r.score, 1, r.root,r.path))

      Iterator((triplet.dstId, toIn), (triplet.srcId, toOut))
    }else{
      Iterator.empty
    }
  }

  def mergeMsgIn(a: List[MsgFlag], b: List[MsgFlag]): List[MsgFlag] = a ++ b

}
