package com.lm.graphx


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexId
/**
  * @Classname GraphxDemo6
  * @Description TODO
  * @Date 2020/5/25 13:55
  * @Created by limeng
  */
object GraphxDemo6 {

  /**
    * 节点数据更新 集合union
    * @param vid
    * @param vdata
    * @param message
    * @return
    */
  def vprog(vid:VertexId,vdata:Int,message:Int): Int ={
      math.min(vdata,message)
  }

  def sendMsg(e:EdgeTriplet[Int,Int]):Iterator[(org.apache.spark.graphx.VertexId, Int)]={
     if(e.srcAttr != Int.MaxValue && e.srcAttr + e.attr < e.dstAttr){
       println(s"${e.srcAttr}--${e.attr}--${e.dstAttr}")
       return Iterator(e.dstId,e.srcAttr+e.attr).asInstanceOf[Iterator[(org.apache.spark.graphx.VertexId, Int)]]
     }
     Iterator.empty
  }

  /**
    * 节点数据更新 集合union
    * @param
    * @param vdata
    * @param message
    * @return
    */
  def mergeMsg(vdata:Int,message:Int): Int ={
     math.min(vdata,message)
  }


  def main(args: Array[String]): Unit = {
      val session = SparkSession.builder().master("local[*]").appName("GraphxDemo6").getOrCreate()

      val sc = session.sparkContext

    val vertexRDD3 =  sc.makeRDD(Array(
      (1L,("zhang1",25)),
      (2L,("zhang2",18)),
      (3L,("zhang3",45)),
      (4L,("zhang4",20)),
      (5L,("zhang5",70)),
      (6L,("zhang6",79)),
      (7L,("zhang7",101))
    ))


    val edgesRDD3=sc.makeRDD(Array(
      Edge(1L,2L,"1"),
      Edge(3L,2L,"1"),
      Edge(1L,4L,"1"),
      Edge(7L,2L,"1"),
      Edge(3L,5L,"1"),
      Edge(7L,6L,"1"),
      Edge(6L,4L,"1")
    ))


    val vertexRDD4 =  sc.makeRDD(Array(
      (1L,("test1",61)),
      (2L,("test2",62)),
      (3L,("test3",63))
    ))


    val edgesRDD4=sc.makeRDD(Array(
      Edge(1L,2L,"test1"),
      Edge(3L,2L,"test1")
    ))

    val graph3: Graph[(String, Int), String] = Graph(vertexRDD3, edgesRDD3)


    val graph4: Graph[(String, Int), String]  = Graph(vertexRDD4,edgesRDD4)

    println("原貌：")
    graph3.triplets.collect().foreach(triplet=>{
              println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr} --edge=${triplet.attr} --dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} ")
          })

    println("aggregateMessages:")

    /**
      * 聚合到尾结点，去在所有起始节点找最大
      */
    val graph3Value: VertexRDD[Int] = graph3.aggregateMessages[Int](triplet => {
      triplet.sendToDst(triplet.srcAttr._2)
    },
      (a, b) => math.max(a, b))

    graph3Value.collect().foreach(println(_))


    println("joinVertices:")
    graph3.joinVertices(vertexRDD4)((v, vd, u) => {
      (u._1, u._2)
    }).triplets.collect().foreach(triplet=>{
      println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr} --edge=${triplet.attr} --dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} ")
    })



    println("outerJoinVertices:")
    graph3.outerJoinVertices(vertexRDD4)((v, vd, u) => {
      u.getOrElse(null)
    }).triplets.collect().foreach(triplet=>{
      println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr} --edge=${triplet.attr} --dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} ")
    })



    println("Pregel:")

    println("最短路径：")
    /**
      *  EdgeDirection.out：出的方向
      * EdgeDirection.in:入的方向
      * EdgeDirection.Either：入边或出边
      * EdgeDirection.Both：入边且出边
      *
      *  def pregel
      * (
      *   initialMsg:A //从图初始化的时候，开始模型计算的时候，所有节点都会收到一个默认消息
      *   maxIterations: Int = Int.MaxValue,//最大的迭代次数
      *   activeDirection: EdgeDirection = EdgeDirection.Either)//发送消息的方向
      *   (vprog: (VertexId, VD, A) => VD,//节点调用该消息将聚合后的数据和本节点进行属性的合并
      *   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],//激活态的节点调用这个方法发送消息
      *   mergeMsg: (A, A) => A)//如果一个节点收到多条消息，那么就会使用mergeMsg将消息合并为一条消息，如果只接收到一条消息，则不合并
      * )
      */
    //最短路径算法:计算的是从V1到所有顶点的最短路径
    val vertexRDD5:RDD[(VertexId,Int)] = sc.makeRDD(Array(
      (1L,0),
      (2L,Int.MaxValue),
      (3L,Int.MaxValue),
      (4L,Int.MaxValue),
      (5L,Int.MaxValue),
      (6L,Int.MaxValue),
      (7L,Int.MaxValue),
      (8L,Int.MaxValue),
      (9L,Int.MaxValue)
    ))
    val edgesRDD5 = sc.makeRDD(Array(
      Edge(1L,2L,6),
      Edge(1L,3L,3),
      Edge(1L,4L,1),
      Edge(3L,2L,2),
      Edge(3L,4L,2),
      Edge(2L,5L,1),
      Edge(5L,4L,6),
      Edge(5L,6L,4),
      Edge(6L,5L,10),
      Edge(5L,7L,3),
      Edge(5L,8L,6),
      Edge(4L,6L,10),
      Edge(6L,7L,2),
      Edge(7L,8L,4),
      Edge(9L,5L,2),
      Edge(9L,8L,3)
    ))

    val graph5: Graph[Int, Int] = Graph(vertexRDD5, edgesRDD5)

    val ccGraph  = graph5.connectedComponents()
    println("连通图 edges:")
   // ccGraph.edges.collect().foreach(println(_))
    println("连通图 vertices:")
    ccGraph.vertices.collect().foreach(println(_))

    val graph6: Graph[node, Int] = graph5.mapVertices((id, attr) => {
      node(attr,List(id))
    })



//    /**
//      * 单源最短
//      */
    val pathGraph: Graph[Int, Int] = graph5.pregel[Int](Int.MaxValue)( vprog = (vid,a,b) =>math.min(a,b),triplet=>{
      //如果三元组的边的权重+入口的值小于目的的值，那么就发送消息，反之不发送
      if(triplet.srcAttr!= Int.MaxValue && triplet.srcAttr+triplet.attr<triplet.dstAttr){
        //println(s"id:${triplet.srcId} --dsid:${triplet.dstId} -- ${triplet.srcAttr}--${triplet.attr}--${triplet.dstAttr}")
        Iterator((triplet.dstId,triplet.srcAttr+triplet.attr))
      }else{
        //不发送消息
        Iterator.empty
      }
    },(a,b)=>math.min(a,b))
    val r1 = pathGraph.triplets.collect()

//    r1.foreach( triplet =>
//      println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}--edge=${triplet.attr}--dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} ")
//    )

    //println(pathGraph.vertices.collect.mkString("\t"))

    println("vertices")

    pathGraph.vertices.collect().foreach(println(_))



  }


}
case class node(value:Int,ids:List[Long]){
  override def toString: String = {
    s"value:$value"
  }
}
