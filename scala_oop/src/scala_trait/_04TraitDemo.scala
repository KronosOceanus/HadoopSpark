package scala_trait

object _04TraitDemo {

  trait Logger{
    def log(msg: String): Unit = println(msg)
  }

  class UserService

  def main(args: Array[String]): Unit = {
    val userService1 = new UserService
    val userService2 = new UserService with Logger
    userService2.log("为对象混入方法")
  }
}
