package scala_partfunc

object _01PartFuncDemo {

  val pf: PartialFunction[Int, String] = {
    case 1 => "一"
    case 2 => "二"
    case _ => "其他"
  }

  def main(args: Array[String]): Unit = {
    print(pf(5))
  }
}
