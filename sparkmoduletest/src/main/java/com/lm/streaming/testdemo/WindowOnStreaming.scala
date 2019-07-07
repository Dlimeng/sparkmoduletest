package demo

import com.lm.demo
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 最近20秒内，读取单词个数
  * @Author: limeng
  * @Date: 2019/6/23 18:01
  */
object WindowOnStreaming {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().master("local[*]").appName("MyUDAF").getOrCreate()
    val sc=ss.sparkContext
    val context = new StreamingContext(sc,Seconds(5))

    val lines: ReceiverInputDStream[String] = context.socketTextStream("node1",9999)

    val words: DStream[(String, Int)] = lines.flatMap(_.split(",")).map((_,1))
    //窗口宽度 20 窗口滑动10
    val rbkw: DStream[(String, Int)] = words.reduceByKeyAndWindow(_+_,_-_,Seconds(20),Seconds(10))

    rbkw.print()
    rbkw.saveAsTextFiles("","")

    context.start()
    context.awaitTermination()
    context.stop()
    ss.stop()

  }
}
