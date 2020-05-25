package com.lm.graphx

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname GraphxDemo5
  * @Description TODO
  * @Date 2020/5/18 20:15
  * @Created by limeng
  */
object GraphxDemo5 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[*]")
      .appName("test1").getOrCreate()
    val sc = session.sparkContext
    val dataSource=List("1 1","2 1","2 2","3 2",
      "4 3","5 3","5 4","6 4","6 5","7 5",
      "8 7","9 7","9 8","9 9","10 8","11 9")

    val rdd = sc.parallelize(dataSource).map(x=>{
       val data = x.split(" ")
      (data(0).toLong,data(1).toInt)
    }).cache()

    /**
      * (1,1)
      * (2,1)
      * (2,2)
      * (3,2)
      * (4,3)
      * (5,3)
      * (5,4)
      * (6,4)
      * (6,5)
      * (7,5)
      * (8,7)
      * (9,7)
      * (9,8)
      * (9,9)
      * (10,8)
      * (11,9)
      */
    //rdd.collect().foreach(println(_))

    /**
      * (8,List(7))
      * (1,List(1))
      * (9,List(7, 8, 9))
      * (10,List(8))
      * (2,List(1, 2))
      * (11,List(9))
      * (3,List(2))
      * (4,List(3))
      * (5,List(3, 4))
      * (6,List(4, 5))
      * (7,List(5))
      */
    //rdd.groupBy(_._1).map(x=>{(x._1,x._2.unzip._2)}).collect().foreach(println(_))

    /**
      * zipWithIndex
      * 该函数将RDD中元素和这个元素在RDD中的ID(索引号)组合成键/值对
      */
    val rdd1 = sc.makeRDD(Seq("A","B","C","D","E","F"),2)
    rdd1.zipWithUniqueId().collect().foreach(println(_))
    //同构，返回类型一致
    val l1=List(1,2,3,4)
    l1.reduce(_+_)

    val l = List(1,2,3,4)
    //x 它代指返回值
    //y 是对rdd各个元素遍历
    l.aggregate(0,0)((x,y) =>(x._1+y,x._2+1 ) ,(x,y) => (x._1+y._1,x._2+y._2 ))

  }
}
