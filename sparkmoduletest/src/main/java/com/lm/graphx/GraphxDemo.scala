package com.lm.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/**
  * @Author: limeng
  * @Date: 2019/7/8 13:25
  */
object GraphxDemo {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[1]")
      .appName("test1").getOrCreate()

    val context = session.sparkContext
    val myVertices: RDD[(Long, String)] = context.makeRDD(Array((1L,"Ann"),(2L,"Bill"),(3L,"Charles"),(4L,"Diane"),(5L,"Went")))
    val myEdges: RDD[Edge[String]] = context.makeRDD(Array(Edge(1L,2L,"is"),Edge(2L,3L,"is"),Edge(3L,4L,"is"),Edge(4L,5L,"like"),Edge(3L,5L,"wrote")))

    val myGraph: Graph[String, String] = Graph(myVertices,myEdges)
    //出度
    val vertices1:VertexRDD[Int] = myGraph.aggregateMessages[Int](_.sendToDst(1),_+_)
    println("vertices1:")
    vertices1.collect().foreach(println(_))
    println()

    //使用pregel 找到最远距离
   // Pregel(myGraph.mapVertices((vid,vd) => 0),0,activeDirection = EdgeDirection.Out)


    val vertexRDD3 = context.makeRDD(Array(
      (1L,("zhang1",25)),
      (2L,("zhang2",18)),
      (3L,("zhang3",45)),
      (4L,("zhang4",20)),
      (5L,("zhang5",70)),
      (6L,("zhang6",79)),
      (7L,("zhang7",101))
    ))

    val edgesRDD3=context.makeRDD(Array(
      Edge(1L,2L,"1"),
      Edge(3L,2L,"1"),
      Edge(1L,4L,"1"),
      Edge(7L,2L,"1"),
      Edge(3L,5L,"1"),
      Edge(7L,6L,"1"),
      Edge(6L,4L,"1")
    ))


    val graph3 = Graph(vertexRDD3,edgesRDD3)
    graph3.aggregateMessages[Int](
      triplet=>{
        triplet.sendToDst(triplet.srcAttr._2)
      },
      (a,b)=>math.max(a,b)
    )

  //  println("vertices2 :")
  //  vertices2.collect().foreach(println(_))

    //创建顶点数据集
    val vertexRDD:RDD[(VertexId,(String,String))] = context.makeRDD(Array(
      (3L,("zhangsan","student")),
      (7L,("limeng","学生")),
      (5L,("lisi","老师")),
      (2L,("wangwu","老师")),
      (4L,("zhaoliu","老师")),
      (6L,("wangguo","老师"))
    ))
    //创建边的数据
    val edgesRDD:RDD[Edge[String]] = context.makeRDD(Array(
      Edge(3L,7L,"测试关系"),
      Edge(5L,3L,"测试指导"),
      Edge(2L,5L,"测试朋友"),
      Edge(5L,7L,"测试朋友")
    ))

    //构建一个图
    val  graphx = Graph(vertexRDD,edgesRDD)

//    graphx.triplets.collect().foreach(triplet=>{
//        println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr} --edge=${triplet.attr} --dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} ")
//    })
//
//
//    //边的数量
//    graphx.numEdges
//    //顶点的数量
//
//    graphx.numVertices
//    //获取所有顶点的入度
//    println("inDegrees:")
//    graphx.inDegrees.collect().foreach(println(_))
//    //获取所有顶点的出度
//    println("outDegrees:")
//    graphx.outDegrees.collect().foreach(println(_))
//
//
//    //转换操作
//    println("转换操作：")
//    println("mapVertices: ")
//    graphx.mapVertices((id,a) => (a._1,a._2))
//    graphx.vertices.collect().foreach(println(_))
//    graphx.edges.collect().foreach(println(_))
//
//    graphx.triplets.foreach(f=>{
//
//    })
//
//    //结构操作
//    println("mapEdges: ")
//    graphx.mapEdges(attr=>{
//      1
//    }) .triplets
//      .collect
//      .foreach(triplet => println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}--edge=${triplet.attr}--dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))
//
//    //聚合操作
//    println("mapTriplets: ")
//    graphx.mapTriplets(e=>{
//      s"${e.srcAttr}:${e.dstAttr}:${e.attr}"
//    }).triplets
//      .collect
//      .foreach(triplet => println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}  edge=${triplet.attr}  dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))
//
//
//    println("mapVertices :")
//    graphx.mapVertices((id,attr)=>attr._1)
//      .triplets
//      .collect
//      .foreach(triplet => println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}  edge=${triplet.attr}  dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))
//
//    println("reverse: ")
//    graphx.reverse.triplets
//      .collect
//      .foreach(triplet => println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr} edge=${triplet.attr}  dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))
//
//    //
//    println("subgraph: ")
//    //满足三元组边判断，满足顶点的判定
//    graphx.subgraph(x=>if(x.attr=="测试朋友") true else false,(id,attr)=>true).triplets
//      .collect
//      .foreach(triplet => println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}  edge=${triplet.attr}  dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))
//
//
//    println("mask: ")
//
//    /**
//      * mask操作构造一个子图，类似于交集，这个子图包含输入图中包含的顶点和边。
//      * 它实现很简单，顶点和边均做inner join操作
//      * 顶点和边都做交集，边属性一样，顶点属性一样
//      */
//    //创建边的数据
//
//    val validGraph = graphx.subgraph(vpred = (id, attr) => attr._2 != "student")
//    graphx.mask(validGraph).triplets
//      .collect
//      .foreach(triplet => println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}  edge=${triplet.attr}  dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))
//
//
//    val graphx3 = graphx.mask(validGraph)
//    println("mask edges:")
//    graphx3.edges.collect().foreach(println(_))
//    println("mask vertices:")
//    graphx3.vertices.collect().foreach(println(_))
//
//
//    //连通图
//    /**
//      * 每个顶点多一个属性。这属性表示就是这个顶点所在连通图中最小顶点
//      */
//    val ccGraph  = graphx.connectedComponents()
//    println("连通图 edges:")
//    ccGraph.edges.collect().foreach(println(_))
//    println("连通图 vertices:")
//    ccGraph.vertices.collect().foreach(println(_))
//
    //groupEdges 根据边分组，合并多重图中并行边（如顶点对之间重复的边）
//    println("groupEdges:")
//    graphx.partitionBy(PartitionStrategy.RandomVertexCut,1).groupEdges((e1,e2)=>e1+e2).triplets.collect().foreach(triplet=>println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}  edge=${triplet.attr}  dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))

    // collectNeighbors 聚合 该方法的作用是收集每个顶点的邻居顶点的顶点id和顶点属性
//    println("collectNeighbors:")
    /**
      * EdgeDirection.out：出的方向
      * EdgeDirection.in:入的方向
      * EdgeDirection.Either：入边或出边
      * EdgeDirection.Both：入边且出边
      *
      * 收集邻居节点数据，边的另一个顶点
      */
//    graphx.collectNeighbors(EdgeDirection.Either).collect().foreach(f=>{
//      val colls : Array[(VertexId, (String, String))] = f._2
//      println(s"VertexId:${f._1} ${colls.foreach(println(_))}")
//    })
//
//
//    println("collectNeighborIds")
//
//    graphx.collectNeighborIds(EdgeDirection.Either).collect().foreach(f=>{
//      val colls :Array[(VertexId)] = f._2
//      println(s"VertexId:${f._1} ${colls.foreach(println(_))}")
//    })
//
//    println("aggregateMessages:")


  }

  def sendMsg(ec:EdgeContext[Int,String,Int]):Unit={
    ec.sendToDst(ec.srcAttr+1)
  }

  def mergeMsg(a:Int,b:Int):Int={
    math.max(a,b)
  }
  //  def  propagateEdgeCount(g:Graph[Int,String]):Graph[Int,String]={
  //
  //  }
}
