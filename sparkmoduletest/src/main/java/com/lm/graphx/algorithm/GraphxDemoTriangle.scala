package com.lm.graphx.algorithm

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Edge, _}
/**
  * @Classname GraphxDemoTriangle
  * @Description TODO
  * @Date 2020/6/9 19:54
  * @Created by limeng
  * 三角形
  * 用于社区发现
  * 如微博中你关注的人也关注你，大家的关注关系中有很多三角形，说明社区很强很稳定，大家联系比较紧密；如果一个人只关注了很多人，却没有形成三角形，则说明社交群体很小很松散。
  * 衡量社群耦合关系的紧密程度
  * 通过三角形数量来反应社区内部的紧密程度，作为一项参考指标。
  */
object GraphxDemoTriangle {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("GraphxDemoBreadth").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val g = GraphLoader.edgeListFile(sc,"D:\\工具\\workspace\\sparkmoduletest\\sparkmoduletest\\src\\main\\resources\\files\\Slashdot0811.txt").cache()
    val g2 = Graph(g.vertices,g.edges.map(e=>{
      if(e.srcId < e.dstId) e else new Edge(e.dstId,e.srcId,e.attr)
    })).partitionBy(PartitionStrategy.RandomVertexCut)


//    g2.triplets
//      .collect
//      .foreach(triplet => println(s"srcId=${triplet.srcId} srcAttr=${triplet.srcAttr}--edge=${triplet.attr}--dstId=${triplet.dstId} dstAttr=${triplet.dstAttr} "))

    val seq = (0 to 6).map(i => {
      g2.subgraph(vpred = (vid, _) => vid >= i * 10000 && vid < (i+i) * 10000).triangleCount().vertices.map(m=>{
        m._2
      }).reduce(_+_)
    })


    session.stop()
  }

}
