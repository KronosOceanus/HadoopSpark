package scala_partfunc

object _02PartFuncDemo {

  val list1 = (1 to 10).toList

  val list2 = list1.map{
    case x if x >= 1 && x <= 3 => "[1-3]"
    case x if x >= 4 && x <= 8 => "[4-8]"
    case _ => "(8-*]"
  }

  def main(args: Array[String]): Unit = {
    println(list2)
  }
}
