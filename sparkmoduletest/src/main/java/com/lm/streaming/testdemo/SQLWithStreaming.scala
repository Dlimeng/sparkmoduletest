package demo

import com.lm.demo
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * spark streaming sparksql
  *
  * @Author: limeng
  * @Date: 2019/6/25 19:36
  */
object SQLWithStreaming {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().master("local[*]").appName("MyUDAF").getOrCreate()
    val sc=ss.sparkContext
    val context = new StreamingContext(sc,Seconds(5))


    val lines: ReceiverInputDStream[String] = context.socketTextStream("node1",9999)

    val words: DStream[String] = lines.flatMap(_.split(","))

    //每分钟计算最近一小时的数据量
    val windowWords = words.window(Minutes(60),Seconds(15))


  }
}
