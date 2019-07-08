package com.lm.service

import com.lm.dao.UserRepositoryComponent

/**
  * @Author: limeng
  * @Date: 2019/7/8 17:47
  */
trait UserServiceComponent {
  this: UserRepositoryComponent=>
  val userService: UserService = new UserServiceClassImpl
  userService.userRepository =this.userRepository
  userService.userRepositoryUpdate = this.UserRepositoryUpdate
}
