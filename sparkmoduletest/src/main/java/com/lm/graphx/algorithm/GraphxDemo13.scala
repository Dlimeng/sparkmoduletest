package com.lm.graphx.algorithm

import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo13
  * @Description TODO
  * @Date 2020/7/29 15:40
  * @Created by limeng
  */
object GraphxDemo13 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo13").master("local[*]").getOrCreate()
    val sc = session.sparkContext

    // cid gid
    val mm = sc.makeRDD(Array((3L,1L),(3L,2L),(3L,4L),(2L,2L),(2L,4L)))

    val root = sc.makeRDD(Array((1L,1L),(2L,2L),(4L,4L)))

    val bc = sc.broadcast(root.collectAsMap())

    val filterRdd =  mm.mapPartitions(iter=>{
      val value = bc.value
      for {m <- iter if(value.contains(m._1) && m._2 != m._1)} yield (m._1,m._1)
    })

    filterRdd.collect().foreach(println(_))


  }
}
