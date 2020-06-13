package com.lm.spark

import org.apache.spark.sql.SparkSession

/**
  * @Classname SparkTest3
  * @Description TODO
  * @Date 2020/6/11 10:33
  * @Created by limeng
  *  fold aggregate()
  *
  */
object SparkTest3 {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo10").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val l = List(1,2,3,4)

    //x代指返回值，y对rdd各元素遍历
     val lvalue1: Int = l.fold(1)((x, y) =>x+y)

     println(s"fold :${lvalue1}")

    /**
      * * 先对每个分区的元素做聚集，然后对所有分区的结果做聚集
    */
    val lvalue2: (Int, Int) = l.aggregate(0,0)((x, y)=>(x._1+y,x._2+1) , (x, y) =>(x._1+y._1,x._2+y._2))

    println(s"aggregate: ${lvalue2._1} , ${lvalue2._2}")




    session.stop()
  }

}
