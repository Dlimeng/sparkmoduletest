package com.lm.graphx.algorithm



import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo21
  * @Description TODO
  * @Date 2021/1/19 15:47
  * @Created by limeng
  */
class GraphxDemo21 {

}
object GraphxDemo21{
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo14").master("local[*]").getOrCreate()



    val sc = session.sparkContext

    val root: RDD[(Long, Long)] = sc.makeRDD(Array((2L, 2L), (8L, 8L), (7L, 7L)))

    val v = sc.makeRDD(Array((1L,0),(2L,0),(3L,0),(4L,0),(5L,0),(6L,0),(7L,0),(8L,0)))


    val value: RDD[Edge[Double]] = sc.makeRDD(Array(Edge(2L, 1L, 40D), Edge(3L, 1L, 20D), Edge(4L, 1L, 60D), Edge(5L, 1L, 20D),
      Edge(4L, 3L, 60D), Edge(2L, 3L, 40D), Edge(2L, 4L, 20D), Edge(6L, 4L, 40D),Edge(5L, 4L, 40D),Edge(2L, 6L, 40D),Edge(5L, 6L, 40D),Edge(7L, 6L, 20D),Edge(8L, 5L, 40D)))
    val e =value

    val graph: Graph[Int, Double] = Graph(v, e)

    val subGraphs = graph.outerJoinVertices(root)((vid,vd,ud)=>{
      val u = ud.getOrElse(Long.MaxValue)
      if(u != Long.MaxValue){
        InAndOut(List(FromInfo(vid, true,null)), List[FromInfo]())
      }else{
        InAndOut(List(FromInfo(vid, false,null)), List[FromInfo]())
      }
    }).pregel(List[MsgFlag](), 10)(vprogIn, sendMsgIn, mergeMsgIn)
      .mapVertices((id, vd) => vd.in.filter(_.srcId != id))


    subGraphs.vertices.filter(_._2.nonEmpty).map(m=>(m._1,m._2.filter(_.root).distinct.sortBy(_.path.split("#").length))).foreach(println(_))

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

}
