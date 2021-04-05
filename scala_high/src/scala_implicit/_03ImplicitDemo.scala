package scala_implicit

object _03ImplicitDemo {
  //元组参数 delimit(String, String)
  def show(name: String)(implicit delimit:(String, String)) = println(s"${delimit._1}:${name}:${delimit._2}")

  object Imp{
    implicit val delimit_default: (String, String) = "<<<" -> ">>>" //隐式参数默认值
  }

  def main(args: Array[String]): Unit = {
    import Imp.delimit_default

    //需要隐式参数的方法
    show("张三")
    show("李四")("((", "))")
  }
}
