package com.lm.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @Classname JedisPoolUtils
  * @Description TODO
  * @Date 2020/9/7 15:10
  * @Created by limeng
  */
object JedisPoolUtils{
  var  pool:JedisPool = null

  {
    if(pool == null){
      val config = new JedisPoolConfig()
      config.setMaxIdle(25)
      config.setMaxWaitMillis(1000 * 100)
      config.setTestOnBorrow(true)
      pool = new JedisPool(config,"192.168.200.116",6379,2000,"know!@#123KNOW",10)
    }
  }

  def getConnection() ={
    pool.getResource
  }

  def closeConnection(jedis:Jedis): Unit ={
      if(jedis != null){
        jedis.close()
      }
  }

}