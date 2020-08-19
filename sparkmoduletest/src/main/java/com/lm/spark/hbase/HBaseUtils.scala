package com.lm.spark.hbase

import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.{Base64, Bytes}
/**
  * @Classname HBaseUtils
  * @Description TODO
  * @Date 2020/8/3 19:40
  * @Created by limeng
  */
object HBaseUtils {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  private var connection:Connection = null

  private var conf:Configuration = null

  def init(): Unit ={
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","hbase")
    connection = ConnectionFactory.createConnection(conf)
  }

  def getJobConf(tableName:String): Unit ={
    val conf = HBaseConfiguration.create()

    val jobConf = new JobConf(conf)

    jobConf.set("hbase.zookeeper.quorum", "hbase")

    jobConf.set("hbase.zookeeper.property.clientPort", "2181")

    jobConf.set(org.apache.hadoop.hbase.mapred.TableOutputFormat.OUTPUT_TABLE,tableName)

    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])

    jobConf
  }

  def getNewConf(tableName:String) = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbase")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE,tableName)
    val scan = new Scan()
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN,Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
    conf
  }

  def getNewJobConf(tableName:String) = {

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbase")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    conf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.setClass("mapreduce.job.outputformat.class", classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[String]],
      classOf[org.apache.hadoop.mapreduce.OutputFormat[String, Mutation]])
    new JobConf(conf)
  }

  def closeConnection(): Unit = {
    connection.close()
  }
}
