package com.lm.dao

import com.lm.dao.impl.{MockUserRepository, UserRepositoryUpdateImpl}

/**
  * @Author: limeng
  * @Date: 2019/7/8 17:45
  */
trait UserRepositoryComponent {
  protected def userRepository:UserRepository = new MockUserRepository
  protected def UserRepositoryUpdate:UserRepositoryUpdate = new UserRepositoryUpdateImpl
}
