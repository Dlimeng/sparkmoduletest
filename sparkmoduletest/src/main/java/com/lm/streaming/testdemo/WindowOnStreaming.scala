package demo

import com.lm.demo
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming._

/**
  * 最近20秒内，读取单词个数
  * @Author: limeng
  * @Date: 2019/6/23 18:01
  */
object WindowOnStreaming {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().master("local[*]").appName("MyUDAF").getOrCreate()
    val sc=ss.sparkContext

    sc.setCheckpointDir("E:\\tmp\\log")


    val context = new StreamingContext(sc,Seconds(5))


    val lines: ReceiverInputDStream[String] = context.socketTextStream("localhost",9999)

    val words = lines.flatMap(_.split(",")).map((_,1))


    //窗口宽度 20 窗口滑动10
    val rbkw = words.reduceByKeyAndWindow(_+_,_-_,Seconds(20),Seconds(10))

    rbkw.print()

    context.start()
    context.awaitTermination()
    context.stop()
    ss.stop()

  }
}
