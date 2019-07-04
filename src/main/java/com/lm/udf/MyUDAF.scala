package com.lm.udf

import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @Author: limeng
  * @Date: 2019/6/23 15:32
  */
class MyUDAF extends UserDefinedAggregateFunction{
  //输入数据类型
  override def inputSchema: StructType =  DataTypes.createStructType(Array(DataTypes.createStructField("input", StringType, true)))
  //聚合操作时，所处理
  override def bufferSchema: StructType = DataTypes.createStructType(Array(DataTypes.createStructField("aaa", IntegerType, true)))
  //最终值返回类型
  override def dataType: DataType = DataTypes.IntegerType

  override def deterministic: Boolean = true
  //为每个分组的数据执行初始化值
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0
  //每个组，有新的值进来，进行分组对应的聚合值计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = buffer(0) = buffer.getAs[Int](0)+1
  //最后merger的时候，在各个节点上聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  //最后返回一个最终聚合值 要和dataType类型一一对应
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)

}
object MyUDAF{
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().master("local[*]").appName("MyUDAF").getOrCreate()
    val sqlc=ss.sqlContext
    val sc=ss.sparkContext

    val rdd=sc.makeRDD(Array("zhangsan","lisi","wangwu","zhangsan","lisi"))
    val rowRDD = rdd.map(x=>{
      RowFactory.create(x)
    })

    val schema = DataTypes.createStructType(Array(DataTypes.createStructField("name", StringType, true)))
    val df = sqlc.createDataFrame(rowRDD,schema)
    df.show()
    df.createOrReplaceTempView("user")
    sqlc.udf.register("StringCount",new MyUDAF)

    sqlc.sql("select name ,StringCount(name) from user group by name")
    ss.stop()
  }
}