package com.lm.graphx

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo4
  * @Description TODO
  * @Date 2020/5/18 15:37
  * @Created by limeng
  *  二跳邻居
  */
object GraphxDemo4 {
  type VMap = Map[VertexId,Int]

  /**
    * 节点数据更新 集合union
    * @param vid
    * @param vdata
    * @param message
    * @return
    */
  def vprog(vid:VertexId,vdata:VMap,message:VMap): Map[VertexId,Int] ={
    println(s"vprog vid:$vid message:$message")
    addMaps(vdata,message)
  }

  def addMaps(spmap1:VMap,spmap2:VMap): VMap ={
    val ids: Set[VertexId] = spmap1.keySet++spmap2.keySet
    ids.map(k=>{
      k ->math.min(spmap1.getOrElse(k,Int.MaxValue),spmap2.getOrElse(k,Int.MaxValue))
    }).toMap
  }

  def merMaps(spmap1:VMap,spmap2:VMap): VMap ={
    println("merMaps")
    val ids: Set[VertexId] = spmap1.keySet++spmap2.keySet
    ids.map(k=>{
      k ->math.min(spmap1.getOrElse(k,Int.MaxValue),spmap2.getOrElse(k,Int.MaxValue))
    }).toMap
  }

  def sendMsg(e:EdgeTriplet[VMap,_]):Iterator[(VertexId,Map[VertexId,Int])] ={

    //两个集合的差集
    val srcMap = (e.dstAttr.keySet -- e.srcAttr.keySet).map(k=>{k->(e.dstAttr(k)-1)}).toMap
    val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map(k=>{k->(e.srcAttr(k)-1)}).toMap

    if(srcMap.size == 0 && dstMap.size == 0) Iterator.empty
    else {
      Iterator((e.dstId, dstMap), (e.srcId, srcMap))
    }
  }


  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[*]")
      .appName("test1").getOrCreate()
    val sc = session.sparkContext
    val edge=List(//边的信息
      (1,2),(1,3),(2,3),(3,4),(3,5),(3,6),
      (4,5),(5,6),(7,8),(7,9),(8,9))

    val edgeRdd = sc.parallelize(edge).map(x=>{
      Edge(x._1.toLong,x._2.toLong,None)
    })

    //构建图 顶点Int
    val g=Graph.fromEdges(edgeRdd,0)
    println("edges:")
    g.edges.collect().foreach(println(_))
    println("vertices:")
    g.vertices.collect().foreach(println(_))
    println()


    /**
      * 使用两次遍历，首先进行初始化的时候将自己的生命值设为2，第一次遍历向邻居节点传播自身带的ID以及生命值为1(2-1)的消息，第二次遍历的时候收到消息的邻居再转发一次
      *
      */
    println("edges:")
    val two=2
    val value: Graph[Map[VertexId, PartitionID], None.type] = g.mapVertices((vid, _) => Map[VertexId, PartitionID](vid -> two))
    val g1 = value
    g1.edges.collect().foreach(println(_))

    /**
      * Edge(1,2,None)
      * Edge(1,3,None)
      * Edge(2,3,None)
      * Edge(3,4,None)
      * Edge(3,5,None)
      * Edge(3,6,None)
      * Edge(4,5,None)
      * Edge(5,6,None)
      * Edge(7,8,None)
      * Edge(7,9,None)
      * Edge(8,9,None)
      */
    println("vertices:")
    g1.vertices.collect().foreach(println(_))
    println()

    /**
      * (8,Map(8 -> 2))
      * (1,Map(1 -> 2))
      * (9,Map(9 -> 2))
      * (2,Map(2 -> 2))
      * (3,Map(3 -> 2))
      * (4,Map(4 -> 2))
      * (5,Map(5 -> 2))
      * (6,Map(6 -> 2))
      * (7,Map(7 -> 2))
      */
    /**
      *  //pregel参数
      *     //第一个参数 Map[VertexId, Int]() ，是初始消息，面向所有节点，使用一次vprog来更新节点的值，由于Map[VertexId, Int]()是一个空map类型，所以相当于初始消息什么都没做
      *     //第二个参数 two，是迭代次数，此时two=2，代表迭代两次（进行两轮的active节点发送消息），第一轮所有节点都是active节点，第二轮收到消息的节点才是active节点。
      *     //第三个参数 EdgeDirection.Out，是消息发送方向，out代表源节点-》目标节点 这个方向    //pregel 函数参数
      *     //第一个函数 vprog，是用户更新节点数据的程序，此时vprog又调用了addMaps
      *     //第二个函数 sendMsg，是发送消息的函数，此时用目标节点的map与源节点的map做差，将差集的数据减一；然后同样用源节点的map与目标节点的map做差，同样差集的数据减一
      *     //第一轮迭代，由于所有节点都只存着自己和2这个键值对，所以对于两个用户之间存在变关系的一对点，都会收到对方的一条消息，内容是（本节点，1）和（对方节点，1）这两个键值对
      *     //第二轮迭代，收到消息的节点会再一次的沿着边发送消息，此时消息的内容变成了（自己的朋友，0）
      *    //第三个函数 addMaps, 是合并消息，将map合并（相当于求个并集），不过如果有交集（key相同），那么，交集中的key取值（value）为最小的值。
      */
    val newG=g1.pregel(Map[VertexId,Int](),two,EdgeDirection.Out)(vprog,sendMsg,merMaps)
    println("newG vertices")
    newG.vertices.collect().foreach(println(_))
//    println()
//    println("twoJumpFirends vertices")
//    val twoJumpFirends: VertexRDD[Iterable[VertexId]] = newG.vertices.mapValues(_.filter(_._2 == 0).keys)
//    twoJumpFirends.collect().foreach(println(_))

  }


}
