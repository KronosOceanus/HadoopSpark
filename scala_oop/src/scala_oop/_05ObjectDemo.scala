package scala_oop

import java.text.SimpleDateFormat
import java.util.Date

object _05ObjectDemo {

  object DataUtil{
    private val formatter = new SimpleDateFormat("yyyy-MM-dd")

    def format(date: Date): String = formatter.format(date)
  }

  def main(args: Array[String]): Unit = {
    val now = new Date()

    println(DataUtil.format(now))
  }

}
