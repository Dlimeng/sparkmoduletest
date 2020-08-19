package com.lm.spark

import com.lm.spark.model.{GroupMem, MemRel, Node}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @Classname Members
  * @Description TODO
  * @Date 2020/7/16 16:19
  * @Created by limeng
  */
object Members {
  case class mem(targetId: Long,regcap: Double = 0D) extends Serializable
  def getMembersBySplitRoot(memRdd:RDD[GroupMem],nodes: RDD[Node]) : Unit ={
    //val newme = memRdd.filter(f=>f.rel.size > 1000)
    //println(s"大于1000成员 ${newme.count()}")

    val nodeIds = nodes.map(m=>{(m.longId,m.regcap)})

    memRdd.flatMap(f=>{
     val gid = f.groupId
      f.rel.map(m=>(gid,Set[MemRel](MemRel(m.from,m.to,0d,1))))
    }).reduceByKey((a,b) => a++b)
      .foreach(f=>{
        println(f._1+"  "+f._2.toString())

      })



//    memRdd.flatMap(m => {
//      val fromIds = m.rel.map((_.from))
//      val toIds = m.rel.map(_.to)
//      val ids = fromIds.union(toIds)
//      ids.map((_, m.groupId))
//    }).join(nodeIds)
//      .map(f=>{
//        //grouid,id,regcap
//        (f._2._1,(Set[mem](mem(f._1,f._2._2)),f._2._2))
//      }).reduceByKey((a,b)=> (a._1++b._1 ,a._2+b._2))
//      .filter(f=>f._2._2 > 0D).flatMap(f=>{
//      val total = f._2._2
//      val gid = f._1
//      f._2._1.map(m=>{
//        (gid,m.targetId,m.regcap,total)
//      })
//    }).filter(f=> f._3/f._4 > 0.1D)
//      .map(_._2).distinct()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Members").setMaster("local[1]")
    val spark=SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
//
//    val memRdd = sc.parallelize(List[GroupMem](GroupMem(1L,1L,10d,1,1,Set[MemRel](MemRel(1L,2L,10d,1),MemRel(1L,3L,10d,1),MemRel(1L,4L,10d,1),MemRel(1L,5L,10d,1)))
//      ,GroupMem(2L,2L,10d,1,1,Set[MemRel](MemRel(2L,3L,10d,1),MemRel(2L,4L,10d,1),MemRel(2L,5L,10d,1)))
//      ,GroupMem(2L,2L,10d,1,1,Set[MemRel](MemRel(2L,8L,10d,1),MemRel(2L,4L,10d,1),MemRel(2L,5L,10d,1)))))
//
//    val node = sc.parallelize(List[Node](Node("1","1",1L,true,1d,"1")
//    ,Node("2","2",2L,true,2d,"2")
//    ,Node("3","3",3L,true,3d,"3")
//    ,Node("4","4",4L,true,4d,"4")
//    ,Node("5","5",5L,true,5d,"5")))

//    getMembersBySplitRoot(memRdd,node)

//    val v:String = "香港中央结算(代理人)有限公司"
//    println(v.contains("香港中央结算(代理人)有限公司"))

    val map = new mutable.HashMap[String,String]
    map.put("1","2")
    println(map)
    map.clear()
    println(map)
    spark.stop()
  }
}
