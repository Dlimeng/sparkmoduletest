package com.lm.dao.impl

import com.lm.dao.{User, UserRepository}

/**
  * @Author: limeng
  * @Date: 2019/7/8 17:44
  */
class MockUserRepository extends UserRepository {
  def create(user: User): Unit = {
    println( "MockUserRepository creating user: " + user)
  }

  def find(name: String): Unit =  println( "MockUserRepository find user: " + name)

  def update(user: User): Unit =  println( "MockUserRepository update user: " + user)

  def delete(user: User): Unit =  println( "MockUserRepository delete user: " + user)
}
