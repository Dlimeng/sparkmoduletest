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

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[*]")
      .appName("test1").getOrCreate()
    //（id,结果集）
    val vertices = Array((1L,("SFO")),(2L,("ORD")),(3L,("DFW")))
    val context = session.sparkContext
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      context.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      context.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(users, relationships, defaultUser)

    val graph2 = graph.vertices.filter{case (id,(name,pos)) => pos == "postdoc"}.count()

    val graph3 = graph.edges.filter(e=>e.srcId > e.dstId).count()




  }
}
