package scala_oop

object _10OverrideDemo {

  class Person{
    var name: String = "Person"
    val age: Int = 5  //子类只能重写 val 类型的变量

    def getName(): String = this.name
    def getAge(): Int = this.age
  }

  class Student extends Person{
    override val age: Int = 100

    override def getName(): String = s"hello:${super.getName()}"
  }

  def main(args: Array[String]): Unit = {
    val student = new Student
    println(student.age)
    println(student.getName())
  }

}
