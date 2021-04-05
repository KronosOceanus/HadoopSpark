package scala_implicit

object _04ImplicitDemo {
  //元组参数 delimit(String, String)
  def show(name: String)(implicit delimit:(String, String)) = println(s"${delimit._1}:${name}:${delimit._2}")


  def main(args: Array[String]): Unit = {
    implicit val delimit_default: (String, String) = "<<<" -> ">>>" //隐式参数默认值

    show("张三")
    show("李四")("((", "))")
  }
}
