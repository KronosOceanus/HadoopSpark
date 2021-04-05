package scala_oop

object _02ClassObject {

  class Person(var name: String = "", var age: Int = 0) {
    println("主构造器")
  }


  def main(args: Array[String]): Unit = {
    val person = new Person("张三", 20)
    println(person.name)
    println(person.age)
  }
}
