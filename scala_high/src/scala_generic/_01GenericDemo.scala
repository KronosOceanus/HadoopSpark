package scala_generic

object _01GenericDemo {

  class Super

  class Sub extends Super

  class Temp1[+T]

  class Temp2[-T]

  class Temp3[T]


  def main(args: Array[String]): Unit = {
    val t1: Temp1[Super] = new Temp1[Sub]
    val t2: Temp2[Sub] = new Temp2[Super]
    //    val t2: Temp2[Super] = new Temp2[Sub]
    //    val t2: Temp3[Super] = new Temp3[Sub]
  }
}
