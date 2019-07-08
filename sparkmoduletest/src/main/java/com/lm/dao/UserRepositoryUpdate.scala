package com.lm.dao

/**
  * @Author: limeng
  * @Date: 2019/7/8 17:42
  */
trait UserRepositoryUpdate {
  def createUpdate(user: User)

  def findUpdate(name: String)

  def updateUpdate(user: User)

  def deleteUpdate(user: User)
}
