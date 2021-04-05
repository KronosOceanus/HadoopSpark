package scala_oop

import scala_oop._09ExtendsDemo.Student

object _11IsInstanceOfDemo {

  class Person

  class Student extends Person

  def main(args: Array[String]): Unit = {
    val student = new Student

    student match {
      case student1: Person =>  //可强转
        println(student1)
      case _ =>
    }

    println(student.getClass == classOf[Student]) //精确判断，不包括父类
  }
}
