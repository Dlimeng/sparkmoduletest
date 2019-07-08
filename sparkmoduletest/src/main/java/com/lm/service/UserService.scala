package com.lm.service

import com.lm.dao.{User, UserRepository, UserRepositoryUpdate}

/**
  * @Author: limeng
  * @Date: 2019/7/8 17:46
  */
trait UserService {
  var userRepositoryUpdate:UserRepositoryUpdate =_
  var  userRepository:UserRepository =_
  //var
  def create(user: User)
  def find(name: String)
}
