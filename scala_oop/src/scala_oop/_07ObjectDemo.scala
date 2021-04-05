package scala_oop

import scala_oop._07ObjectDemo.CustomerService.SERVICE_NAME

object _07ObjectDemo {
  //伴生类
  class CustomerService{
    private val SERVICE_AGE = 2
    private[this] val SERVICE_TIME = 4  //该修饰符伴生对象无法访问

    def save(): Unit = println(s"${SERVICE_NAME}:保存客户")
  }

  //伴生对象
  object CustomerService{
    private val SERVICE_NAME = "CustomerService"

    def save2(c: CustomerService): Unit = {  //伴生对象中使用伴生类的方法时，需要传入对象
      println(s"${c.SERVICE_AGE}:服务器耐久")
    }

    def apply(): CustomerService = new CustomerService()  //必须写在伴生对象中，就可以不用 new 快速创建对象
  }

  def main(args: Array[String]): Unit = {
    val service = CustomerService()
    service.save()
    CustomerService.save2(service)
  }
}
