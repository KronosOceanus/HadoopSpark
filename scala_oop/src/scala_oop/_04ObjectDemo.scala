package scala_oop

object _04ObjectDemo {

  object Dog{
    //类似 static
    val LEG_NUM = 4

    def printLegNum(): Unit = {
      print(this.LEG_NUM)
    }
  }

  def main(args: Array[String]): Unit = {
    println(Dog.LEG_NUM)
    Dog.printLegNum()
  }
}
