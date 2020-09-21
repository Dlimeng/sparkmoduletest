package com.lm.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.json4s.JsonDSL._
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization

import scala.collection.mutable
/**
  * @Classname HbaseUtils
  * @Description TODO
  * @Date 2020/9/4 17:25
  * @Created by limeng
  */
object HbaseUtils {

  case class MsgScore(from:Long,to:Long,score:Double)  extends Serializable

  lazy val logger: Logger = Logger.getLogger(this.getClass().getName())

  val hbaseQuorum = "prd-hadoop-03.xbox,prd-hadoop-04.xbox,prd-hadoop-05.xbox"
  val hbaseClientPort = "2181"

  implicit val formats  =  Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
    val table = getTable("group_member")
    val set = Array(MsgScore(1L,2L,10D),MsgScore(3L,1L,10D))

    val map = new mutable.HashMap[String,Set[Long]]()
    map.put("test1",Set(1L))
    map.put("test2",Set(2L,3L,5L))


    val values: String = write(map.toMap)

    val mapP: Map[String, Set[Long]] = parse(values).extract[Map[String,Set[Long]]]
    println(mapP)

//    addByRow(table,"1#4","mem","rel",values)
//    addByRow(table,"1#4","mem","group","4")
//    addByRow(table,"3#4","mem","rel",values)
//    addByRow(table,"3#4","mem","group","4")
//
//    scan(table)
  }


  def getHbaseConf(): Configuration  ={
    var hbaseConf: Configuration = null
    try {
      hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", hbaseQuorum)
      hbaseConf.set("hbase.zookeeper.property.clientPort", hbaseClientPort)
    } catch {
      case e: Exception => logger.error("==========连接hbase失败:," + e)
    }
    hbaseConf
  }

  def getTable(tableName:String): Table ={
    var table:Table = null
    try {
      val hbaseConf = getHbaseConf()
      val conn = ConnectionFactory.createConnection(hbaseConf)
      table = conn.getTable(TableName.valueOf(tableName))
    }catch {
      case e:Exception => logger.error("获取Table对象失败:"+e)
    }
    table
  }

  def addByRow(table:Table,rowKey:String,family:String,column:String,value:String)= {
    val rowPut = new Put(Bytes.toBytes(rowKey))
    rowPut.addColumn(family.getBytes,column.getBytes,value.getBytes)
    table.put(rowPut)
  }


  def scan(table:Table): Unit ={
    val scan = new Scan()
    val resultScanner  = table.getScanner(scan)
    val it = resultScanner.iterator()
    while (it.hasNext){
      val result = it.next()
      val row = result.getRow
      println(new String(row))
      println(new String(result.getValue(Bytes.toBytes("mem"),Bytes.toBytes("rel"))))
      println(new String(result.getValue(Bytes.toBytes("mem"),Bytes.toBytes("group"))))
    }
  }


  def dataSetSave(): Unit ={
    val index = Array(1,2)




  }



}
