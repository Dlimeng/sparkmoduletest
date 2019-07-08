package com.lm.service

import com.lm.dao.User

/**
  * @Author: limeng
  * @Date: 2019/7/8 17:47
  */
class UserServiceClassImpl extends UserService {
  def create(user: User): Unit ={
    userRepositoryUpdate.createUpdate(user)
    println("UserServiceClass create")
    userRepository.create(user)
  }
  def find(name:String): Unit ={
    println("UserServiceClass find")
    userRepository.find(name)
  }
}
