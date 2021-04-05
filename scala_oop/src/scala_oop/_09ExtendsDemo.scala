package scala_oop

object _09ExtendsDemo {

  class Person{
    var name: String = _

    def getName(): String = this.name
  }

  object Student extends Person

  def main(args: Array[String]): Unit = {
    Student.name = "张三"
    println(Student.getName())
  }
}
