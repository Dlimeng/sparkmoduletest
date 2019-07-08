package com.lm.dao.impl

import com.lm.dao.{User, UserRepositoryUpdate}

/**
  * @Author: limeng
  * @Date: 2019/7/8 17:44
  */
class UserRepositoryUpdateImpl extends UserRepositoryUpdate{
  override def createUpdate(user: User): Unit = {
    println("UserRepositoryUpdateImpl createUpdate")
  }

  override def findUpdate(name: String): Unit = {
    println("UserRepositoryUpdateImpl findUpdate")
  }

  override def updateUpdate(user: User): Unit = {
    println("UserRepositoryUpdateImpl updateUpdate")
  }

  override def deleteUpdate(user: User): Unit = {
    println("UserRepositoryUpdateImpl deleteUpdate")
  }
}
