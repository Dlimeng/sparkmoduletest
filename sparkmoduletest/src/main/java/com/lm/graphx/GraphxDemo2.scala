package com.lm.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo2
  * @Description TODO
  * @Date 2020/5/13 17:46
  * @Created by limeng
  */
object GraphxDemo2 {

  def main (args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[*]")
      .appName("test1").getOrCreate()
    //（id,结果集）
    val vertices = Array((1L,("SFO")),(2L,("ORD")),(3L,("DFW")))
    val context = session.sparkContext
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      context.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges 边 起始点，目标点，数据
    val relationships: RDD[Edge[(String,String)]] =
      context.parallelize(Array(Edge(3L, 7L, ("collab","testcollab")),    Edge(5L, 3L, ("advisor","testadvisor")),
        Edge(2L, 5L, ("colleague","testcolleague")), Edge(5L, 7L, ("pi","testpi"))))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(users, relationships, defaultUser)


    val vertices1 = graph.vertices.filter{case (id,(name,pos)) => pos == "postdoc"}.count()

    val edges1 = graph.edges.filter(e=>e.srcId > e.dstId).count()

    //VertexId   srcAttr dstArrtr 源属性 目标属性  扩张edge attr//边的目标属性
    val factes: RDD[String] = graph.triplets.map(triplet=>triplet.srcAttr._1 + " is the "+triplet.attr + " of "+triplet.dstAttr._1)

    //factes.collect().foreach(println(_))

    //运算符汇总表
    //边数
    graph.numEdges
    //节点数
    graph.numVertices
    //入度
    graph.inDegrees.collect().foreach(println(_))
    //出度
    graph.outDegrees.collect().foreach(println(_))
    //求出所有的点的度
    graph.degrees.collect().foreach(println(_))



    //使用joinVertices操作，用user中的属性替换图中对应Id的属性 先将图中的顶点属性置空
    //根据给定的另一个图（原图的每个顶点id至多对应此图的一个顶点id）把原图中的顶点属性值根据指定的mapFunc函数进行修改，返回一个新图，新图的顶点类型不变.

    //mapVertices修改顶点属性
    val modifyValue = graph.mapVertices((vid:VertexId,attr:(String,String))=>attr._1 + attr._2)
    //
    println("mapVertices")
    modifyValue.vertices.collect().foreach(println(_))

    println("mapEdges")
    graph.mapEdges(edge => edge.attr._1).edges.foreach(println(_))


    val users2: RDD[(VertexId, (String, Int))] =
      context.parallelize(Array((3L, ("zzzrxin", 30)), (7L, ("zzzzjgonzal", 70)),
        (5L, ("zzzfranklin", 50)), (2L, ("zzzistoica", 20))))

    println("joinVertices")

    val joinVerticesValue = graph.mapVertices((id,attr) => "").joinVertices(users2){(vid,empty,user) =>user._2.toString}

    joinVerticesValue.vertices.collect().foreach(println(_))


    val relationships2: RDD[Edge[(Int)]] =
      context.parallelize(Array(Edge(3L, 7L, 10),    Edge(5L, 3L, 10),
        Edge(2L, 5L, 10), Edge(5L, 7L, 10)))

   // val graph2 = Graph(users2, relationships2, defaultUser)
    //使用mapReduceTriplets来生成新的VertexRDD
    //利用map对每个三元组进行操作
    //利用reduce对相同的Id的顶点属性进行操作

    //使用subgraph生成子图
    //使用groupEdges用来合并相同的ID的边

    //reverse反转，起始点和目标点

    //subgraph 对顶点和边同时限制
//    val subgraphValue = graph2.subgraph(epred = (ed) => (ed.srcId ==3L || ed.dstId == 7L),vpred = (id,attr) => id != 3L)
//    subgraphValue.edges.collect().foreach(println(_))
//    subgraphValue.vertices.collect().foreach(println(_))
    println("连通图------------------------------------")
    val vertexIdValue: RDD[(VertexId, Int)] = context.parallelize(Array((4L,1),(6L,1),(2L,1),(1L,1),(3L,1),(7L,1),(5L,1)))

    val edgeValue = context.parallelize(Array(Edge(1L,2L,1),Edge(2L,3L,1),Edge(3L,1L,1),Edge(4L,5L,1),Edge(4L,5L,1),Edge(5L,6L,1),Edge(6L,7L,1)))

    val graph3 = Graph(vertexIdValue,edgeValue)
    val ccValue: Graph[Long, Int] = graph3.connectedComponents()
    //取连通图，连通图以图中最小Id作为label给图中顶点打属性
    ccValue.vertices.collect().foreach(println(_))

    println("-----------------------")
  }
}
