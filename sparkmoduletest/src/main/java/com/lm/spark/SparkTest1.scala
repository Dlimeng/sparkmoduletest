package com.lm.spark

import org.apache.spark.sql.SparkSession

/**
  * @Classname SparkTest1
  * @Description TODO
  * @Date 2020/5/28 17:55
  * @Created by limeng
  */
object SparkTest1 {
  case class Book(title : String ,pages : Int)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[1]")
      .appName("test1").getOrCreate()

    val context = session.sparkContext

    val arrys= context.makeRDD(Array((1L,"A"),(2L,"B"),(1,"B")))


    arrys.map(m=> (m._1,Set(m._2))).reduceByKey((a,b)=> a ++ b).collect().foreach(println(_))

//      val books = Seq(
//        Book("Future of Scala developer",85),
//        Book("Parallel algorithms", 240),
//        Book("Object Oriented Programming", 130),
//        Book("Mobile Development", 495)
//      )
//
//    val book: Book = books.maxBy(b=>b.pages)
  }
}
