package com.lm.redis

import com.lm.util.JedisPoolUtils
import org.junit.Test
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * @Classname JedisDemo
  * @Description TODO
  * @Date 2020/9/7 15:22
  * @Created by limeng
  */
object JedisDemo {
  val SET_IF_NOT_EXIST= "NX"
  val SET_WITH_EXPIRE_TIME = "PX"
  val LOCK_SUCCESS="OK"




  def disSet(jedis:Jedis,lockKey:String,value:String,expireTime:Int): Unit ={
    val result = jedis.set(lockKey,value,SET_IF_NOT_EXIST,SET_WITH_EXPIRE_TIME,expireTime)

  }

  def exist(jedis:Jedis,key:String) ={
    jedis.exists(key)
  }


  def getLockValue(key:String,jedis:Jedis): String ={
    var value:String = null
    if(jedis != null){
      value = jedis.get(key)
    }
    value
  }

  def delKey(key:String,jedis:Jedis): Unit ={
    if(jedis != null){
      jedis.del(key)
    }
  }


  def main(args: Array[String]): Unit = {

    val jedis = JedisPoolUtils.getConnection()

    val gidMap = "COMPANY_MEMBER_GID_LIST"

//    val str = jedis.get(gidMap)
//    if(str == null){
//      println("kong")
//    }
   // disSet(jedis,gidMap,"test1",10000)
    //disSet(jedis,gidMap,"test2",10)
    //delKey(gidMap,jedis)


   // disSet(jedis,gidMap,"test2",10000)
    import collection.JavaConverters._
    val map = new mutable.HashMap[String,String]()
    map.put("t1","v1")
    map.put("t2","v2")
    map.put("t3","v3")
    jedis.hmset(gidMap,map.asJava)



    jedis.close()
  }
}
