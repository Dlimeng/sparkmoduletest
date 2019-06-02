package com.lm

import com.lm.similar.Simhash


/**
  * @Author: limeng
  * @Date: 2019/6/2 16:03
  */
object demo {
  def main(args: Array[String]): Unit = {
    val content="批处理：指事先将用户程序和数据装入卡带或磁带，并由计算机按照一定的顺序读取，使用户所执行这些程序和数据能够一并批量得到处理的方式。"
    val content2="批处理：指事先将用户程序和数据装入卡带或磁带，并由计算机按照一定的顺序读取，使用户所执行这些程序和数据能够一并得到处理的方式。"
    val simhash=new Simhash
    //576671930010099734
    //1045050699101040694
    val v1 = simhash.calSimhash(content2)
    println(v1)
  }
}
