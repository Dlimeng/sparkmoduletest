package com.lm.analysis

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.types.{DoubleType, FloatType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression

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

    val df = scc.createDataFrame(rdds, schema)

    //预处理
    //1.处理房价
    //3.去除无关值，比如经纬度
    /**
      *
      * 每个家庭的平均房间数：roomsPerHousehold
      * 每个家庭的平均人数：populationPerHousehold
      * 卧室在总房间的占比：bedroomsPerRoom
      */
    val df2=df.withColumn("medianHouseValue12",df("medianHouseValue")/100000).withColumn("roomsPerHousehold", df("totalRooms")/df("households"))
      .withColumn("populationPerHousehold", df("population")/df("households"))
      .withColumn("bedroomsPerRoom", df("totalBedRooms")/df("totalRooms"))

    /**
      * 该区域房价的中位数,区域内总卧室数,区域内总人口数,区域内总家庭数,区域内人均收入中位数
      * 每个家庭的平均房间数,每个家庭的平均人数,卧室在总房间的占比
      */
    import org.apache.spark.sql.functions._
    val names="medianHouseValue12,totalBedrooms,population,households,medianIncome,roomsPerHousehold,populationPerHousehold,bedroomsPerRoom"
   df2.select(names.split(",").map(name=>col(name).cast(DoubleType)):_*).createOrReplaceTempView("tmp2")



    val inputData=scc.sql("select * from tmp2").rdd.map(f=>{
      val v:Vector=Vectors.dense(Array(f.getDouble(1),f.getDouble(2),f.getDouble(3),f.getDouble(4),f.getDouble(5),f.getDouble(6),f.getDouble(7)))
      (f.getDouble(0),v)
    })

    val df3: DataFrame = scc.createDataFrame(inputData).toDF("label", "features")

    //setWithMean是否减均值。setWithStd是否将数据除以标准差。这里就是没有减均值但有除以标准差
    //features训练模型，scaledFeatures 标准化
    val scaler =new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithMean(false).setWithStd(true)
    //计算均值方差等参数
    val scalerModel=scaler.fit(df3)
    //标准化
    val scalerModelDf=scalerModel.transform(df3)

    //---------------------------------------------------------------预处理完成
    //随机切割数据，weights权重，抽样的seed
    val splits : Array[Dataset[Row]] = scalerModelDf.randomSplit(Array(0.8,0.2),123)
     val trainData = splits(0)
     val testData = splits(1)

    // 建立模型，预测谋杀率Murder
    // 设置线性回归参数
    val lr1 = new LinearRegression()
    val lr2 = lr1.setFeaturesCol("scaledFeatures").setLabelCol("label").setFitIntercept(true)
    // RegParam：正则化
    val lr3 = lr2.setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lr = lr3
    //训练集带入模型训练
    val linearNodel = lr.fit(trainData)
    val predicted = linearNodel.transform(testData)


    //真实
    //predicted.select("label").take(1).foreach(println(_))
    //预测值
    //predicted.select("prediction").take(1).foreach(println(_))

    // 模型进行评价
    val trainingSummary = linearNodel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    //残差
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


    println("输出预测结果")
    val predict_result: DataFrame = predicted.selectExpr("scaledFeatures", "label", "round(prediction,1) as prediction")
    predict_result.take(2).foreach(println(_))
    sparkSession.stop()
  }

}
