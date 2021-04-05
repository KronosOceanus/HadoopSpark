package scala_oop

object _08ExtendsDemo {

  class Person{
    var name: String = _

    def getName(): String = this.name
  }

  class Student extends Person

  def main(args: Array[String]): Unit = {
    val student = new Student
    student.name = "张三"
    println(student.name)
    println(student.getName())
  }
}

