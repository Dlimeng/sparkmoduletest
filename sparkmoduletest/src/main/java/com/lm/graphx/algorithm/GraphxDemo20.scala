package com.lm.graphx.algorithm

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo20
  * @Description TODO
  * @Date 2020/10/10 13:46
  * @Created by limeng
  */
object GraphxDemo20 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("GraphxDemo20")
      .master("local[*]")
      .getOrCreate()

    val sc = session.sparkContext



    val v = sc.makeRDD(Array((10L,0),(2L,0),(3L,0),(4L,0),(5L,0),(6L,0),(7L,0),(8L,0)))

    val e = sc.makeRDD(Array(Edge(10L, 4L, 100D), Edge(4L, 5L, 40D), Edge(5L, 7L, 40D), Edge(2L, 3L, 100D),
      Edge(3L, 6L, 30D), Edge(6L, 7L, 40D), Edge(8L, 7L, 20D)))

    val graph: Graph[Int, Double] = Graph(v, e)

    val roots = graph.aggregateMessages[(Int, Int)](ctx => {
      ctx.sendToSrc((1, 0))
      ctx.sendToDst((0, 1))
    }, mergeMs).filter(_._2._2 == 0)

    println("roots")
    roots.collect().foreach(println(_))


    val graphTmp = graph.outerJoinVertices(roots)({
      case (id, _, r) => {
        InAndOut(List(FromInfo(id, 100D, !r.isEmpty)), List[FromInfo]())
      }
    }).pregel(List[MsgFlag](), 10)(vprogIn, sendMsgIn, mergeMsgIn)
      .mapVertices((id, vd) => vd.in.filter(_.srcId != id))
      .cache()

    println("分组")
    graphTmp.vertices.collect().foreach(println(_))
    println(" 得到跟节点对每个点投资比例 ")




    val rootScore = graphTmp.vertices.mapValues((_, vd) => vd.filter(_.root))
      .filter(f => !f._2.isEmpty)
      .flatMap(r => {
         r._2.groupBy(_.srcId).map(ss => {
           //id  (rootId  score)
           (r._1, (ss._1, ss._2.map(_.score).sum))
         })
      }).union( sc.makeRDD(Array((7L,(8L,20D)),(7L,(2L,12D)))))

    println(" rootScore ")
    rootScore.collect().foreach(println(_))

  //  println(" rootScore union ")
  //  rootScore.union( sc.makeRDD(Array((7L,(8L,20D)),(7L,(2L,12D))))).collect().foreach(println(_))

    println(" 初步计算最大股东 ")
    val svs = graphTmp.aggregateMessages[List[Long]](ctx => {
      ctx.sendToDst(List(ctx.srcId))
    }, _ ++ _)

    println("svs")
    svs.collect().foreach(println(_))

    val es = graphTmp.outerJoinVertices(svs)({
      case (_, vd, ss) => (vd, ss.getOrElse(List[Long]()))
    }).vertices.filter(!_._2._1.isEmpty).flatMap(r=>{
      r._2._2
      val ls = r._2._1.groupBy(_.srcId).map(s => {
        (s._1, s._2.map(_.score).sum)
      })
      val max = ls.maxBy(_._2)
      val lsMax = ls.filter(_._2 == max._2)
      val dd = lsMax.filter(f => r._2._2.contains(f._1))
      if (lsMax.size == 1) {
        List((lsMax.head._1, r._1, lsMax.head._2, true))
      } else if (!dd.isEmpty) {
        dd.map(s => (s._1, r._1, s._2, false))
      } else {
        lsMax.map(s => (s._1, r._1, s._2, false))
      }
    })

    es.collect().foreach(println(_))

    val esHand = es.filter(!_._4).map(r => Edge(r._1, r._2, r._3))

    /**
      * (4,5,40.0)
      * (3,6,30.0)
      * (5,7,40.0)
      */
    val esTwo = Graph.fromEdges(esHand, 0).outerJoinVertices(rootScore)({
      case (_, _, rt) => rt.getOrElse((0l, 0d))
    }).aggregateMessages[List[(Long, Long, Double, Double)]](ctx => {
      ctx.sendToDst(List((ctx.srcId, ctx.srcAttr._1, ctx.srcAttr._2, ctx.attr)))
    }, _ ++ _).flatMap(r => {
      val group = r._2.groupBy(_._2).map(f => (f._1, f._2.map(_._3).sum))
      val max = group.maxBy(_._2)
      val maxGroup = group.filter(_._2 == max._2).map(_._1).toList
      r._2.filter(f => maxGroup.contains(f._2)).map(s => (s._1, r._1, s._4))
    })

    println("esTwo")
    esTwo.collect().foreach(println(_))

  }


  def mergeMs(a: (Int, Int), b: (Int, Int)): (Int, Int) = (a._1 + b._1, a._2 + b._2)

  def sendMsgIn(triplet: EdgeTriplet[InAndOut, Double]): Iterator[(VertexId, List[MsgFlag])] = {
    var tm = triplet.srcAttr.in diff (triplet.srcAttr.out)
    //恶心的环
    if (!tm.isEmpty && tm.map(_.srcId).contains(triplet.dstId)) tm = tm.diff(triplet.dstAttr.in)

    if (!tm.isEmpty) {
      val toIn = tm.map(r => {
        val s = r.score * triplet.attr / 100D
        MsgFlag(r.srcId, s, 0, r.root)
      })
      val toOut = tm.map(r => MsgFlag(r.srcId, r.score, 1, r.root))
      Iterator((triplet.dstId, toIn), (triplet.srcId, toOut))
    } else Iterator.empty
  }


  def vprogIn(vertexId: Long, vd: InAndOut, news: List[MsgFlag]): InAndOut = {
    if (news == null || news.isEmpty) vd else {
      val in = vd.in ++ news.filter(_.flag == 0).map(r => FromInfo(r.srcId, r.score, r.root))
      if (vd.out == null) {
        val out = news.filter(_.flag == 1).map(r => FromInfo(r.srcId, r.score, r.root))
        InAndOut(in, out)
      } else {
        val out = vd.out ++ news.filter(_.flag == 1).map(r => FromInfo(r.srcId, r.score, r.root))
        InAndOut(in, out)
      }
    }
  }

  //src 的信息
  case class FromInfo(srcId: Long, score: Double, root: Boolean) extends Serializable {
    override def hashCode(): Int = srcId.hashCode()

    override def equals(obj: scala.Any): Boolean = {
      if (obj == null) false else {
        val o = obj.asInstanceOf[FromInfo]
        o.srcId.equals(this.srcId)
      }
    }

    override def toString: String = srcId + "#" + score.formatted("%.3f") + "#" + root
  }


  def mergeMsgIn(a: List[MsgFlag], b: List[MsgFlag]): List[MsgFlag] = a ++ b


  case class InAndOut(in: List[FromInfo], out: List[FromInfo]) extends Serializable

  // flag 1 给OUT  0 给In
  case class MsgFlag(srcId: Long, score: Double, flag: Int, root: Boolean) extends Serializable {
    override def toString: String = srcId + " # " + flag
  }
}
