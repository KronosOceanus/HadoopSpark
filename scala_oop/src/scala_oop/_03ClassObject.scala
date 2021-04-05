package scala_oop

object _03ClassObject {

  class Customer(var name: String = "", var address: String = ""){
    def this(data: Array[String]){
      this(data(0), data(1))    //调用主构造器或者其他辅助构造器
    }

  }

  def main(args: Array[String]): Unit = {
    val customer = new Customer(Array("张三", "北京"))
    println(customer.name)
    println(customer.address)
  }
}
