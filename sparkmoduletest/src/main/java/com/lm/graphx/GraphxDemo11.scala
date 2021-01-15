package com.lm.graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo11
  * @Description TODO
  * @Date 2020/9/24 16:24
  * @Created by limeng
  */
object GraphxDemo11 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemoBreadth").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val v = sc.makeRDD(Array((3L,"A"),(2L,"B"),(5L,"E"),(7L,"G")))

    val e =sc.makeRDD(Array(Edge(3L,7L,5.0),Edge(5L,3L,3.0),Edge(2L,5L,10.0),Edge(5L,7L,2.0)))

    val graph: Graph[String, Double] = Graph(v, e)

    graph.mapVertices((vid,vd) => if(vid == 2L) 0.0 else Double.MaxValue).pregel[Double](Double.MaxValue)( vprog = (vid,a,b) =>math.min(a,b),triplet=>{
      //如果三元组的边的权重+入口的值小于目的的值，那么就发送消息，反之不发送
      if(triplet.srcAttr!= Double.MaxValue && triplet.srcAttr+triplet.attr<triplet.dstAttr){
        Iterator((triplet.dstId,triplet.srcAttr+triplet.attr))
      }else{
        //不发送消息
        Iterator.empty
      }
    },(a,b)=>math.min(a,b)).vertices.collect().foreach(println(_))

  }
}
