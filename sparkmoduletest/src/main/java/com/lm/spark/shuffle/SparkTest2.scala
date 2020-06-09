package com.lm.spark.shuffle

import org.apache.spark.sql.SparkSession

/**
  * @Classname SparkTest2
  * @Description TODO
  * @Date 2020/6/5 10:26
  * @Created by limeng
  * 采样倾斜key并分拆join操作
  */
object SparkTest2 {
  type MVType = (Int,Double)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("GraphxDemo10").master("local[*]").getOrCreate()

    val sc = session.sparkContext

    val value = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))), 3)

    value.sample(false,0.1).collect().foreach(println(_))

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)


    d1.combineByKey(
      score=>(1,score),
      (c1:MVType,newScore) =>(c1._1+1,c1._2+newScore),
      (c1:MVType,c2:MVType) => (c1._1+c2._1,c1._2+c2._2)
    ).map({case (name,(num,score)) =>(name,score/num)}).collect().foreach(println(_))



    session.stop()
  }
}
