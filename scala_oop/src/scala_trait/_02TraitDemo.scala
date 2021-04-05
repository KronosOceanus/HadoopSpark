package scala_trait

import java.text.SimpleDateFormat
import java.util.Date

object _02TraitDemo {

  trait Logger {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val TYPE: String

    def log(msg: String)
  }

  class Console extends Logger{
    override val TYPE: String = "控制台消息"

    override def log(msg: String): Unit = println(s"${TYPE}:${formatter.format(new Date)}:${msg}")
  }

  def main(args: Array[String]): Unit = {
    val console = new Console
    console.log("你是一个好人")
  }
}
