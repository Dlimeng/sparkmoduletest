package com.lm.service.registry

import com.lm.dao.{User, UserRepositoryComponent}
import com.lm.service.UserServiceComponent

/**
  * 蛋糕模型
 *
  * @Author: limeng
  * @Date: 2019/7/8 17:48
  */
object ComponentRegistry {
  def save(): Unit ={
    lazy val newuserService =new UserServiceComponent with UserRepositoryComponent
    newuserService.userService.create(User("limeng"))
  }

  def main(args: Array[String]): Unit = {
    save()
  }
}
