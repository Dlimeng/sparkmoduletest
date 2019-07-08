package com.lm.dao

/**
  * @Author: limeng
  * @Date: 2019/7/8 17:42
  */
trait UserRepository {
  def create(user: User)

  def find(name: String)

  def update(user: User)

  def delete(user: User)
}
case class User (name:String)
