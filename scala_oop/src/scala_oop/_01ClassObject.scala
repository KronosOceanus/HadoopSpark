package scala_oop

object _01ClassObject {

  class Person() {
    private var name: String = _
    var age: Int = _

    def setName(name: String): Unit = this.name = name
    def getName: String = this.name

    def printHello(msg: String): Unit = println(msg)
  }


  def main(args: Array[String]): Unit = {
    val person = new Person
    person.setName("张三")
    person.age = 20
    println(person.getName)
    println(person.age)
    person.printHello("message")
  }
}
