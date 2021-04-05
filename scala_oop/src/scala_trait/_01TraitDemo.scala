package scala_trait

import java.util.logging.Logger

object _01TraitDemo {

  trait Logger{
    def log(msg: String)

    def log2(msg: String) = println(msg)
  }
  trait Debuger{
    def debug(msg: String)
  }

  class Console extends Logger with Debuger {
    override def log(msg: String): Unit = println(msg)

    override def debug(msg: String): Unit = println(msg)
  }

  object ConsoleSingle extends Logger {
    override def log(msg: String): Unit = println(msg)

    def log22(): Unit = log2("默认方法")
  }

  def main(args: Array[String]): Unit = {
    val console = new Console
    console.log("log")
    console.debug("debug")
    ConsoleSingle.log("single")
    ConsoleSingle.log22()
  }
}
