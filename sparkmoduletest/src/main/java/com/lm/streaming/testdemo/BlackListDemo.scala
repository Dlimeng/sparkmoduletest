package demo

import com.lm.demo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 下面模拟使用transform算子和过滤RDD类型的黑名单，使用LeftOuterJoin
  * 如果广播出去的黑名单不是list,可以使用这种方式
  *
  * @Author: limeng
  * @Date: 2019/6/23 19:55
  */
object BlackListDemo {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().master("local[*]").appName("MyUDAF").getOrCreate()
    val sc=ss.sparkContext
    val context = new StreamingContext(sc,Seconds(5))

    val lines: ReceiverInputDStream[String] = context.socketTextStream("localhost",9999)


    val blacklistRDD: RDD[(String, Boolean)] = sc.parallelize(Array("zhangsan2", "lisi2")).map((_, true))

    val words: DStream[(String, Int)] = lines.flatMap(_.split(",")).map((_,1))


    val needwordDS=words.transform(rdd=>{
      val leftRDD = rdd.leftOuterJoin(blacklistRDD)

      val needword=leftRDD.filter( tuple =>{
        val y=tuple._2
        if(y._2.isEmpty){
          true
        }else{
          false
        }})
        needword.map(tuple =>(tuple._1,1))
    })

    val wcDS=needwordDS.reduceByKey(_+_)
    wcDS.print()

    context.start()
    context.awaitTermination()
    context.stop()
    ss.stop()
  }
}
