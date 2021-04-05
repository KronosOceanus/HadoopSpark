package scala_trait

object _03TraitDemo {

  trait Logger{
    def log(msg: String)

    //模板设计模式
    def info(msg: String): Unit = log(s"信息:${msg}")  //具体方法调用抽象方法
    def warn(msg: String): Unit = log(s"警告:${msg}")
    def error(msg: String): Unit = log(s"错误:${msg}")
  }

  class Console extends Logger {
    override def log(msg: String): Unit = println(msg)
  }

  def main(args: Array[String]): Unit = {
    val console = new Console
    console.info("你是一个好人")
    console.warn("你是一个好人")
    console.error("你是一个好人")
  }
}
