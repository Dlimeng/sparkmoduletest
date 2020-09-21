package com.lm.util

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable

/**
  * @Classname JedisUtils
  * @Description TODO
  * @Date 2020/9/4 15:07
  * @Created by limeng
  */
object JedisUtils {




  implicit val formats  =  Serialization.formats(NoTypeHints)

  def getRedisClient(): Jedis ={
    val redisClient = new Jedis("192.168.200.116", 6379)

    redisClient.auth("know!@#123KNOW")

    redisClient
  }

  val keys = "GROUP_MEMBER_LIST_TOP"

  def put(): Unit ={
    val jedis = getRedisClient()

    val map = new mutable.HashMap[String,Set[Long]]()
    val longs: Set[Long] = Set(1L,2L)
    map.put("long",longs)

    val values = write(map)



  // jedis.set(keys,values,-1,10)

  }

  def pool(): Unit ={
    val config = new JedisPoolConfig()
    //JedisPool(GenericObjectPoolConfig poolConfig, String host, int port, int timeout, String password, int database
    val pool = new JedisPool()

  }

}
