package com.lm.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * @Classname GraphxDemo9
  * @Description TODO
  * @Date 2020/5/29 16:49
  * @Created by limeng
  */
object GraphxDemo9 {

  val rootContains = Array("创投","投资","基金","资本")
  //基于控制
  def findRootIds(sc:SparkContext,graph:Graph[String,Int]): RDD[Long] ={

    //识别准集团顶点
    val root = graph.mapVertices((vid, vd)=>(0,0,vd))
      .aggregateMessages[(Int,Int,String)](ctx=>{
        ctx.sendToSrc((0,1,ctx.srcAttr._3))
        ctx.sendToDst((1,0,ctx.dstAttr._3))
      },(a,b)=>(a._1+b._1,a._2+b._2,a._3))
      .filter(_._2._1 == 0)

    //找到包含rootContains关键词的顶点
    val filterRoot = root.mapPartitions(iter => {
      for {(k, v) <- iter if(rootContains.filter(con => v._3.contains(con.toString)).length != 0)}
        yield (k, k)
    })

    val rootMap = filterRoot.collectAsMap()
    if(rootMap.size > 0){
      val rbc = sc.broadcast(rootMap)
      //将包含rootContains关键词的顶点从graph中过滤掉
      val subGraph = graph.subgraph(vpred = (vid,v) => !rbc.value.contains(vid)).cache()
      graph.unpersist()

      //递归寻找顶点
      findRootIds(sc, subGraph)
    }else{
      graph.unpersist()
      root.map(_._1)
    }
  }


  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo9").master("local[*]").getOrCreate()

    val sc = session.sparkContext


    val edge1: RDD[Edge[Int]] = sc.makeRDD(Array(
      Edge(2, 1, 0),
      Edge(4, 1, 0),
      Edge(1, 2, 0),
      Edge(6, 3, 0),
      Edge(7, 3, 0),
      Edge(6, 7, 0),
      Edge(3, 7, 0)
    ))

    val vertexValue: RDD[(VertexId, String)] = sc.makeRDD(Array(
      (1L, "ba1"),
      (2L, "ba2"),
      (3L, "ba3"),
      (4L, "ba4"),
      (6L, "创投"),
      (7L, "ba7"),
      (8L, "ba8")
    ))


    val graph1 : Graph[String, Int] = Graph(vertexValue,edge1)

    sc.stop()
    session.stop()
  }
}
