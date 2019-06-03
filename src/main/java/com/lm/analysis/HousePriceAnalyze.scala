package com.lm.analysis

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.reflect.internal.util.TableDef.Column

/**
  *
  * 数据集 http://www.dcc.fc.up.pt/~ltorgo/Regression/cal_housing.html
  * 房屋普查，预测房价
  * 数据集中的每个数据都代表一块区域内房屋和人口基本信息
  * 1.该地区中心的纬度（latitude）
  * 2.该地区中心的经度（longitude）
  * 3.区域内所有房屋屋龄的中位数（housingMedianAge）
  * 4.区域内总房间数（totalRooms）
  * 5.区域内总卧室数（totalBedrooms）
  * 6.区域内总人口数（population）
  * 7.区域内总家庭数（households）
  * 8.区域内人均收入中位数（medianIncome）
  * 9.该区域房价的中位数（medianHouseValue）
  *
  * A = bB+cC+dD+....+iI ,A代表房价，B到I分别代表另外八个属性
  * 假设影响是线性的
  *
  * 预处理
  * 1.房价值大，调整为小值
  * 2.有的属性没什么意义，比如所有房子的总房间数和总卧室数，我们更加关心的是平均房间数；
  * 3.在我们想要构建的线性模型中，房价是结果，其他属性是输入参数。所以我们需要把它们分离处理；
  * 4.有的属性最小值和最大值范围很大，我们可以把它们标准化处理。
  *
  *
  * @Author: limeng
  * @Date: 2019/6/2 22:18
  */
object HousePriceAnalyze {
  def main(args: Array[String]): Unit = {
    val sparkSession =  SparkSession.builder().appName("HousePriceAnalyze")
      .master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    val scc=sparkSession.sqlContext

    val rdd=sc.textFile("E:\\dataset\\cal_housing\\CaliforniaHousing\\cal_housing.data")

    val header=sc.textFile("E:\\dataset\\cal_housing\\CaliforniaHousing\\cal_housing.domain")

    val schema = StructType(List(
      StructField("longitude", FloatType, nullable = false),
      StructField("latitude", FloatType, nullable = false),
      StructField("housingMedianAge", FloatType, nullable = false),
      StructField("totalRooms", FloatType, nullable = false),
      StructField("totalBedrooms", FloatType, nullable = false),
      StructField("population", FloatType, nullable = false),
      StructField("households", FloatType, nullable = false),
      StructField("medianIncome", FloatType, nullable = false),
      StructField("medianHouseValue", FloatType, nullable = false)
    ))

    val rdds=rdd.map(_.split(",")).map(line=>{
      Row(line(0).toFloat,
        line(1).toFloat,
        line(2).toFloat,
        line(3).toFloat,
        line(4).toFloat,
        line(5).toFloat,
        line(6).toFloat,
        line(7).toFloat,
        line(8).toFloat)}).toJavaRDD()

    var df = scc.createDataFrame(rdds, schema)

    //预处理
    //1.处理房价
    df.withColumn("medianHouseValue",df("medianHouseValue")/100000)

    /**
      * 2
      * 每个家庭的平均房间数：roomsPerHousehold
      * 每个家庭的平均人数：populationPerHousehold
      * 卧室在总房间的占比：bedroomsPerRoom
      */
    df.withColumn("roomsPerHousehold", df("totalRooms")/df("households"))
        .withColumn("populationPerHousehold", df("population")/df("households"))
      .withColumn("bedroomsPerRoom", df("totalBedRooms")/df("totalRooms"))

    //3.去除无关值，比如经纬度
    df.select("medianHouseValue","totalBedrooms","population","households","medianIncome"
      ,"roomsPerHousehold","populationPerHousehold","bedroomsPerRoom").createOrReplaceTempView("tmp1")

    //Vectors.

    scc.sql("select * from tmp1").map(f=>{
      val v=Vectors.dense(f.getDouble(1),f.getDouble(2),
        f.getDouble(3),f.getDouble(4),f.getDouble(5),
        f.getDouble(6),f.getDouble(7))
      (f.getDouble(0),v)
    }).toDF("","")
    //拆分

    //val df2 = scc.createDataFrame(inputData,VectorMode.getClass)
    //println(df2.take(2))
  }

}
case class VectorMode(label:Double,features:Vector)