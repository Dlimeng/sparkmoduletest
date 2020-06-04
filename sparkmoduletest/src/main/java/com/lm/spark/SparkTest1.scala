package com.lm.spark

/**
  * @Classname SparkTest1
  * @Description TODO
  * @Date 2020/5/28 17:55
  * @Created by limeng
  */
object SparkTest1 {
  case class Book(title : String ,pages : Int)

  def main(args: Array[String]): Unit = {
      val books = Seq(
        Book("Future of Scala developer",85),
        Book("Parallel algorithms", 240),
        Book("Object Oriented Programming", 130),
        Book("Mobile Development", 495)
      )

    val book: Book = books.maxBy(b=>b.pages)
  }
}
