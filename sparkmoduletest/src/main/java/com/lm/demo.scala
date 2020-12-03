package com.lm

import com.lm.similar.Simhash

/**
  * @Author: limeng
  * @Date: 2019/6/2 16:03
  */
object demo {
  def main(args: Array[String]): Unit = {
//    val content="批处理：指事先将用户程序和数据装入卡带或磁带，并由计算机按照一定的顺序读取，使用户所执行这些程序和数据能够一并批量得到处理的方式。"
//    val content2="批处理：指事先将用户程序和数据装入卡带或磁带，并由计算机按照一定的顺序读取，使用户所执行这些程序和数据能够一并得到处理的方式。"
//    val content3="批处理：指事先将用户程序和数据装入卡带或磁带，并由计算机按照一定的顺序读取，使用户所执行这些程序和数据能够一并批量得到处理的方式。"
//    val cs=Array(content,content2,content3)
//    val simhash=new Simhash(false)
    //576671930010099734
//    cs.foreach(f=>{
//      val v1 = simhash.calSimhash(f)
//      simhash.store(v1,null)
//    })
//    simhash.isDuplicate(content,null)
    //val v1 = simhash.calSimhash(content)
    //println(v1)

 //   val s ="test#c#vb"
 //   s.split("#")(2)

//    val longExecID = "030520IDEsparkdev-cdh-client1:9106IDE_hdfs_21"
//    val creatorLength = Integer.parseInt(longExecID.substring(0,2))
//    val executeLength = Integer.parseInt(longExecID.substring(2,4))
//    val instanceLength = Integer.parseInt(longExecID.substring(4,6))
//    val creator = longExecID.substring(6, 6 + creatorLength)
//    val executeApplicationName = longExecID.substring(6 + creatorLength, 6 + creatorLength + executeLength)
//    val instance = longExecID.substring(6 + creatorLength + executeLength, 6 + creatorLength + executeLength + instanceLength)
//    val shortExecID = longExecID.substring(6 + creatorLength + executeLength + instanceLength, longExecID.length)

//    println("creator")

     val list = Array(1,2,3,4,5)

     println(list.tail.fold(0)(_ + _))


  }

  def testFUN(): Unit ={
    println("test lm fun ")
  }


}
