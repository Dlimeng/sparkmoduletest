package com.lm.graphx.algorithm


import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{VertexId, _}
/**
  * @Classname GraphxDemoPath
  * @Description TODO
  * @Date 2020/6/11 10:20
  * @Created by limeng
  * 最短路径
  */
object GraphxDemoPath {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemoBreadth").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val v = sc.makeRDD(Array((1L,"A"),(2L,"B"),(3L,"C"),(4L,"D"),(5L,"E"),(6L,"F"),(7L,"G")))

    val e =sc.makeRDD(Array(Edge(1L,2L,7.0),Edge(1L,4L,5.0),Edge(2L,3L,8.0),Edge(2L,4L,9.0),
      Edge(2L,5L,7.0),Edge(3L,5L,5.0),Edge(4L,5L,15.0),Edge(4L,6L,6.0),Edge(5L,6L,8.0)
      ,Edge(5L,7L,9.0),Edge(6L,7L,11.0)))

    val graph: Graph[String, Double] = Graph(v, e)

    /**
      * (1,(A,0.0))
      * (2,(B,7.0))
      * (3,(C,15.0))
      * (4,(D,5.0))
      * (5,(E,14.0))
      * (6,(F,11.0))
      * (7,(G,22.0))
      */
    println("dijkstra:")
    dijkstra(graph,1L).vertices.collect().foreach(println(_))

    /**
      * (1,0.0)
      * (2,7.0)
      * (3,15.0)
      * (4,5.0)
      * (5,14.0)
      * (6,11.0)
      * (7,22.0)
      */
    println("dijkstra pregel")
    graph.mapVertices((vid,vd) => if(vid == 1L) 0.0 else Double.MaxValue).pregel[Double](Double.MaxValue)( vprog = (vid,a,b) =>math.min(a,b),triplet=>{
      //如果三元组的边的权重+入口的值小于目的的值，那么就发送消息，反之不发送
      if(triplet.srcAttr!= Double.MaxValue && triplet.srcAttr+triplet.attr<triplet.dstAttr){
        Iterator((triplet.dstId,triplet.srcAttr+triplet.attr))
      }else{
        //不发送消息
        Iterator.empty
      }
    },(a,b)=>math.min(a,b)).vertices.collect().foreach(println(_))


    /**
      * (1,(A,0.0,List()))
      * (2,(B,7.0,List(1)))
      * (3,(C,15.0,List(1, 2)))
      * (4,(D,5.0,List(1)))
      * (5,(E,14.0,List(1, 2)))
      * (6,(F,11.0,List(1, 4)))
      * (7,(G,22.0,List(1, 4, 6)))
      */
    println("dijkstra pregel 最短路径+距离")
    dijkstra2(graph,1L).vertices.collect().foreach(println(_))

    session.stop()

  }

  /**
    * 最短距离
    * @param g
    * @param origin
    * @return
    */
  def dijkstra(g:Graph[String,Double],origin:VertexId) ={
    var g2: Graph[(Boolean, Double), Double] = g.mapVertices((vid, _) => {
      val vd = if (vid == origin) 0 else Double.MaxValue
      (false, vd)
    })

    //遍历所有点
    (0L until g.vertices.count()).foreach(f=>{
      //确定最短路径值最小作为当前顶点
      val currentVerID = g2.vertices.filter(!_._2._1)
        .fold((0L,(false,Double.MaxValue))){
          (a,b)=> if(a._2._2 < b._2._2) a else b
        }._1
      //向当前顶点相邻的顶点发送消息，聚合，取最小值为最短路径
      val newDistance: VertexRDD[Double] = g2.aggregateMessages[Double](
        ctx => {
          if (ctx.srcId == currentVerID) ctx.sendToDst(ctx.srcAttr._2 + ctx.attr)
        },
        (a, b) => math.min(a, b)
      )
      //生成结果
      g2 = g2.outerJoinVertices(newDistance){
        (vid,vd,newSum) => ((vd._1 || vid == currentVerID),math.min(vd._2,newSum.getOrElse(Double.MaxValue)) )
      }

    })

    val result: Graph[(String, Double), Double] = g.outerJoinVertices(g2.vertices) {
      (vid, vd, dist) => {
        (vd, dist.getOrElse((false, Double.MaxValue))._2)
      }
    }
    result

  }


  def dijkstra2(g:Graph[String,Double],origin:VertexId) ={

    var g2: Graph[(Boolean, Double, List[VertexId]), Double] = g.mapVertices((vid, _) => {
      (false, if (vid == origin) 0 else Double.MaxValue, List[VertexId]())
    })


    //遍历所有点
    (0L until g.vertices.count()).foreach(f=>{
      val currentVerID = g2.vertices.filter(!_._2._1).fold(0L,(false,Double.MaxValue,List[VertexId]()))((a,b)=>{
        if(a._2._2 < b._2._2) a else b
      })._1


      val newDistance: VertexRDD[(Double,List[VertexId])] =g2.aggregateMessages[(Double,List[VertexId])](
        ctx => {
          if (ctx.srcId == currentVerID) {
            ctx.sendToDst((ctx.srcAttr._2 + ctx.attr, ctx.srcAttr._3 :+ ctx.srcId))
          }

        },
        (a,b)=> if(a._1 < b._1)  a else b
      )


      g2 = g2.outerJoinVertices(newDistance){
        (vid,vd,newSum) => {
          val newSumVal = newSum.getOrElse((Double.MaxValue,List[VertexId]()))

          ((vd._1 || vid == currentVerID),
          math.min(vd._2,newSumVal._1),
          if(vd._2 < newSumVal._1 )  vd._3 else newSumVal._2
        )}
      }
    })



    val result: Graph[(String, Double, List[VertexId]), Double] = g.outerJoinVertices(g2.vertices) {
      (vid, vd, dist) => {
        (vd, dist.getOrElse((false, Double.MaxValue, List[VertexId]()))._2, dist.getOrElse((false, Double.MaxValue, List[VertexId]()))._3)
      }
    }
    result

  }


}
