package com.lm.spark.rdd

import org.apache.spark.sql.SparkSession

/**
  * @Classname SparkTest
  * @Description TODO
  * @Date 2020/10/30 16:13
  * @Created by limeng
  *
  * createCombiner: V => C 这个函数把当前的值作为参数，此时我们可以对其做些附加操作（类型转换）并把它返回（这一步类似于初始化操作）
  * mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
  * mergeCombiners: (C, C) => C，该函数把2个元素C 合并（这个操作在不同分区间进行）
  */
object SparkTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[1]")
      .appName("test1").getOrCreate()

    val context = session.sparkContext


    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))

    val d1 = context.parallelize(initialScores)

    type MVType = (Int, Double)

    d1.combineByKey(
      score => (1,score),
      (c1:MVType,newSore) => (c1._1 + 1,c1._2+newSore),
      (c1:MVType,c2:MVType) =>(c1._1 + c2._1, c1._2 + c2._2)
    ).map {
      case (name,(num,score)) => (name, score/num)
    }.collect().foreach(println(_))



    session.stop()
  }
}
