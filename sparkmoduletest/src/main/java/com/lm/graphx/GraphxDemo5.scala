package com.lm.graphx

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

    rdd.groupBy(_._1).map(x=>{(x._1,x._2.unzip._2)})

  }
}
