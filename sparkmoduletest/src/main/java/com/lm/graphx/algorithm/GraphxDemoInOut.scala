package com.lm.graphx.algorithm

import com.lm.graphx.pojo.{FromInfo, InAndOut, MsgFlag}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemoInOut
  * @Description TODO
  * @Date 2020/6/15 18:47
  * @Created by limeng
  * pregel in out 输入输出集合，获取
  * 老版本划分成员
  */
object GraphxDemoInOut {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemoBreadth").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val edgeVal = sc.makeRDD(Array(Edge(1L,2L,100d),Edge(1L,3L,40d),Edge(2L,7L,100d),Edge(2L,6L,20d),Edge(7L,6L,30d),Edge(3L,6L,50d)))

    val graph = Graph.fromEdges(edgeVal,1)


//    val pregelValue = graph.mapVertices((id, _) => {
//      InAndOut(List(FromInfo(id, 100d)), List[FromInfo]())
//    }).pregel(List[MsgFlag](), 10)(vprogIn, sendMsgIn, mergeMsgIn)

    /**
      * (1,InAndOut(List(1#100.000),List(1#100.000, 1#100.000)))
      * (2,InAndOut(List(2#100.000, 1#100.000),List(2#100.000, 2#100.000, 1#100.000, 1#100.000)))
      * (3,InAndOut(List(3#100.000, 1#40.000),List(3#100.000, 1#40.000)))
      * (6,InAndOut(List(6#100.000, 2#20.000, 7#30.000, 3#50.000, 1#20.000, 2#30.000, 1#20.000, 1#30.000),List()))
      * (7,InAndOut(List(7#100.000, 2#100.000, 1#100.000),List(7#100.000, 2#100.000, 1#100.000)))
      */
   // pregelValue.vertices.collect().foreach(println(_))

//    val value = pregelValue.vertices.mapValues((id, vd) => {
//      vd.in.filter(_.srcId != id)
//    })
//      .filter(f => !f._2.isEmpty)
//      .flatMap(r => {
//        r._2.groupBy(_.srcId).map(ss => {
//          Edge(ss._1, r._1, ss._2.map(_.score).sum)
//        })
//      })

    /**
      * Edge(1,2,100.0)
      * Edge(1,3,40.0)
      * Edge(2,6,50.0)
      * Edge(7,6,30.0)
      * Edge(1,6,70.0)
      * Edge(3,6,50.0)
      * Edge(2,7,100.0)
      * Edge(1,7,100.0)
      */
   // value.collect().foreach(println(_))

    session.stop()
  }


//  def sendMsgIn(triplet: EdgeTriplet[InAndOut, Double]): Iterator[(VertexId, List[MsgFlag])]  = {
//    var tmp = triplet.srcAttr.in diff(triplet.srcAttr.out)
//    if(!tmp.isEmpty && tmp.map(_.srcId).contains(triplet.dstId)){
//      tmp = tmp.filter(f=> !triplet.dstAttr.in.map(_.srcId).contains(f.srcId))
//    }
//
//    if(!tmp.isEmpty){
//      val toIn = tmp.map(r=>{
//        val s = r.score * triplet.attr / 100D
//        MsgFlag(r.srcId,s,0)
//      })
//
//      val toOut =  tmp.map(r=>{
//        MsgFlag(r.srcId,r.score,1)
//      })
//      Iterator((triplet.dstId,toIn),(triplet.srcId,toOut))
//    }else{
//      Iterator.empty
//    }
//  }
//
//
//  def  mergeMsgIn(a: List[MsgFlag], b: List[MsgFlag]):List[MsgFlag]= a++b
//
//  def   vprogIn(vertexId: Long, vd: InAndOut, news: List[MsgFlag]): InAndOut = {
//    if(news == null || news.isEmpty) vd else{
//      val in = vd.in ++ news.filter(_.flag == 0).map(r=>FromInfo(r.srcId,r.score))
//      if(vd.out == null || vd.out.isEmpty){
//        val out = news.filter(_.flag == 1).map(r => FromInfo(r.srcId, r.score))
//        InAndOut(in, out)
//      }else{
//        val out = vd.out ++ news.filter(_.flag == 1).map(r => FromInfo(r.srcId, r.score))
//        InAndOut(in, out)
//      }
//    }
//  }
}
