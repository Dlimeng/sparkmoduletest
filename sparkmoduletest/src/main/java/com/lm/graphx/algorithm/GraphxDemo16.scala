package com.lm.graphx.algorithm


import com.lm.graphx.pojo.{FromInfo, InAndOut, MsgFlag}
import com.lm.spark.model.{GroupMem, MemRel}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


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


    val v = sc.makeRDD(Array((1L,0),(2L,0),(3L,0),(4L,0),(5L,0),(6L,0),(7L,0),(8L,0),(9L,0),(10L,0)))

    val e = sc.makeRDD(Array(Edge(1L, 2L, 51D), Edge(1L, 4L, 30D), Edge(6L, 7L, 51D), Edge(6L, 8L, 51D),
      Edge(2L, 4L, 30D), Edge(4L, 5L, 30D), Edge(7L, 4L, 30D), Edge(7L, 5L, 30D), Edge(9L, 10L, 20D)))


    val graph: Graph[Int, Double] = Graph(v, e)

    val roots = graph.aggregateMessages[(Int,Int,VertexId)](ctx=>{
      ctx.sendToSrc((1,0,ctx.srcId))
      ctx.sendToDst((0,1,ctx.srcId))
    },mergeMs)

    val head = roots.filter(f=> f._2._2 == 0).map(m=>(m._1,m._1))


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

    val headSub = subRoots.filter(f=> f._2._2 == 0).map(m=>(m._1,m._1))
    val tailSub = subRoots.filter(f=> f._2._1 == 0).map(m=>(m._1,m._1))
    println("头结点")
    headSub.collect().foreach(println(_))

    //求子图成员
    val childQueryGroup = subGraph.outerJoinVertices(headSub)((vid,vd,ud) =>{
      val u = ud.getOrElse(Long.MaxValue)
      if(u != Long.MaxValue){
        GroupVD(Set(MsgScore(u,u,u,100D)),Set[MsgScore](),Set(vid))
      }else{
        GroupVD(Set[MsgScore](), Set[MsgScore](), Set())
      }
    }).pregel(GroupVD(Set[MsgScore](), Set[MsgScore](), Set[Long]()),
      10)(gProg, gSendMsg2, gMergeMsg)

    val tgids = mm.map(m=> (m.targetId,m.groupId)).groupByKey()

    val intgisFlag = headSub.join(tgids).map(m=> (m._1,m._2._2.toSet))

    //没在mm子集
    val subTgis = headSub.subtract(intgisFlag.map(m=>(m._1,m._1))).map(m=> (m._1,Set(m._1)))
    println("subTgis ")
    subTgis.collect().foreach(println(_))

    val tgisFlag = intgisFlag.union(subTgis)

    println("tgisFlag ")
    tgisFlag.collect().foreach(println(_))

    val childMM = childQueryGroup.vertices.flatMap(f=>{f._2.accept}).filter(f=> f.from != f.to).map(m=> (m.groupId,m)).join(tgisFlag).flatMap(m=>{
      m._2._2.map(p=>{
        MsgScore(p,m._2._1.from,m._2._1.to,m._2._1.score)
      })
    })


    println("childMM ")
    childMM.collect().foreach(println(_))


    val subGraphs = subGraph.outerJoinVertices(headSub)((vid,vd,ud)=>{
      val u = ud.getOrElse(Long.MaxValue)
      if(u != Long.MaxValue){
        InAndOut(List(FromInfo(vid, true,null)), List[FromInfo]())
      }else{
        InAndOut(List(FromInfo(vid, false,null)), List[FromInfo]())
      }
    }).pregel(List[MsgFlag](), 10)(vprogIn, sendMsgIn, mergeMsgIn)
      .mapVertices((id, vd) => vd.in.filter(_.srcId != id))

    println("FromInfos ")
    val subVertices =  subGraphs.vertices.filter(f=> f._2.nonEmpty).map(m=> (m._1,m._2.filter(_.root).distinct.sortBy(_.path.split("#").length).reverse))
    subVertices.collect().foreach(println(_))

//    val tmpMM1 = mm.map(m=>(m.targetId,m))
//    val tmpMM2 = subGraph.vertices.map(m=>(m._1,m._1)).join(tmpMM1).map(m=> m._2._2)

    val tmpMM1  =  tgisFlag.flatMap(m=>{
      m._2.map(m2=>{
        GroupMem(m._1,m2,100D,1,0,Set())
      })
    })

 //   println("tmpMM2 list")
  //  tmpMM2.collect().foreach(println(_))
    //需要准集团关系
    val bMMM = sc.broadcast(tmpMM1.collect())
    val value: Broadcast[Array[VertexId]] = sc.broadcast(subTgis.map(_._1).collect())
    val bsubTgis = value
//
//    val bheadTotal = sc.broadcast(head.map(_._1).collect())
//
//    val subrel2 = subrel.map(m=>{
//      (m._1,m._2,m._3,bheadTotal.value.contains(m._1))
//    })
//
//    //相对关系表
//    val bsubrel = sc.broadcast(subrel2.collect())
    //准集团所有龙头

    //子集团
    val bchildMMM = sc.broadcast(childMM.collect())


    println("tailSub")
    tailSub.collect().foreach(println(_))

    var tailChilds = tailSub.join(subVertices).map(m=> (m._1,m._2._2))

    tailChilds = tailChilds.map(m=> {
      val toid = m._1
      val list = m._2.map(m2=>{
        FromInfo(m2.srcId,m2.root,m2.path+"#"+toid)
      })
      (toid,list)
    })

    println("末尾子图 ")
    tailChilds.collect().foreach(println(_))





  val mapchilds = tailChilds.flatMap(m1=>{
      val tail = m1._1
      val gg =  new mutable.HashMap[String, Set[GroupMem]]

      val maxSize = m1._2.maxBy(_.path.split("#").length).path.split("#").length
      var index:Int = 0
      val remove = new ListBuffer[String]
      while (index < maxSize){
        val ids =m1._2.filter(l=> (l.path.split("#").length-1) >= index).map(m=>{
          (m.srcId,m.path.split("#")(index).toLong)
        })
        index +=1
        ids.foreach(f=>{
          val root = f._1
          val tailid = f._2
          if(root == tailid){
            if(!gg.contains(root.toString)){
              val b = bMMM.value.filter(l=> l.targetId == root)
              if(b.nonEmpty){
                remove.+=(root.toString)
                gg.put(root.toString,b.toSet)
              }
            }

          }else{
            val  list = bchildMMM.value.filter(l=> l.to == tailid).filter(l=> !gg.contains(l.to.toString)).groupBy(_.to)

            list.foreach(f3=>{
              val toid = f3._1

              val gvds = f3._2.groupBy(_.groupId).map(m2=>{
                val gm = m2._2.filter(l=> gg.contains(l.from.toString)).map(m3=>{
                  val from = m3.from
                  val gids = gg.getOrElse(from.toString,null).map(m2=> m2.groupId)
                  (from->gids)
                }).toMap

                val gfrom = m2._2.filter(l=> {
                  val gv = gm.getOrElse(l.from,null)
                  if(gv!= null && gv.contains(l.groupId)){
                    true
                  }else{
                    false
                  }
                }).map(_.from).toList

                val tuples = m2._2.filter(l=>gfrom.contains(l.from))

                val gid = m2._1
                val sumd = tuples.map(m3=> m3.score).sum
                val rels = tuples.map(m3=>MemRel(m3.from,m3.to,m3.score,1))
                GroupMem(toid,gid.toLong,sumd,1,0,rels.toSet)
              }).filter(l=> l.rel.nonEmpty)

              if(gvds.nonEmpty){
                val maxscore = gvds.maxBy(_.score)

                gvds.filter(l2=> maxscore.score == l2.score).toList.foreach(f4=>{
                  val to = f4.targetId
                  val v= gg.getOrElse(to.toString,null)
                  if(v == null){
                    gg.put(to.toString,Set(f4))
                  }else{
                    gg.put(to.toString,(v ++ Set(f4)))
                  }
                })
              }
            })
          }
        })
     }
    if(gg.nonEmpty) {
      remove.foreach(f=> gg.remove(f))
    }
    gg.values.flatten
  }).filter(l=> !bsubTgis.value.contains(l.groupId))

    println("测试子图 ")
    mapchilds.collect().foreach(println(_))

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

  def gSendMsg2(triplet: EdgeTriplet[GroupVD, Double]): Iterator[(Long, GroupVD)] = {
    val from = triplet.srcId
    val to = triplet.dstId
    val unsent = triplet.srcAttr.accept.diff(triplet.srcAttr.sent)

    //获取投资占股50%
    if(unsent.nonEmpty){

      if(!triplet.srcAttr.ids.contains(triplet.dstId)){
        val msg = unsent.map(a=> MsgScore(a.groupId,from,to,triplet.attr))

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
  case class MsgFlag(srcId: Long,  flag: Int, root: Boolean,path:String) extends Serializable {
    override def toString: String = srcId + " # " + flag
  }

  case class FromInfo(srcId:Long, root: Boolean,path:String) extends Serializable{
    override def hashCode(): Int = srcId.hashCode()

    override def equals(obj: Any): Boolean = {
      if(obj == null) false else{
        val o = obj.asInstanceOf[FromInfo]
        o.srcId.equals(this.srcId)
      }
    }

    override def toString: String = srcId+"#"+ root.toString+"#"+path
  }

  case class FromPathInfo(srcId:Long,path:String,paths: List[MsgScore]) extends Serializable{
    override def hashCode(): Int = srcId.hashCode()

    override def equals(obj: Any): Boolean = {
      if(obj == null) false else{
        val o = obj.asInstanceOf[FromPathInfo]
        o.srcId.equals(this.srcId)
      }
    }

    override def toString: String = srcId+"#"+path
  }


  def vprogIn(vertexId: Long,vd:InAndOut,news:List[MsgFlag]):InAndOut ={
    if(news == null || news.isEmpty) vd else{
      val in = vd.in ++ news.filter(_.flag == 0).map(r=> FromInfo(r.srcId,r.root,r.path))
      if(vd.out == null){
        val out = news.filter(_.flag == 1).map(r=> FromInfo(r.srcId,r.root,r.path))
        InAndOut(in,out)
      }else{
        val out = vd.out ++ news.filter(_.flag == 1).map(r => FromInfo(r.srcId,  r.root,r.path))
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
        val p = r.path
        if(p == null) MsgFlag(r.srcId, 0, r.root,r.srcId.toString)
        else MsgFlag(r.srcId, 0, r.root,p+"#"+triplet.srcId)
      })
     val toOut = tm.map(r => MsgFlag(r.srcId, 1, r.root,r.path))

      Iterator((triplet.dstId, toIn), (triplet.srcId, toOut))
    }else{
      Iterator.empty
    }
  }

  def mergeMsgIn(a: List[MsgFlag], b: List[MsgFlag]): List[MsgFlag] = a ++ b

}
