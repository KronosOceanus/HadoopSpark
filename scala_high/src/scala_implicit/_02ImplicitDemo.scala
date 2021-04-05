package scala_implicit

import java.io.File

import scala.io.Source

object _02ImplicitDemo {

  //传入要隐式转换的参数
  class RichFile(file: File){
    def read(): String = Source.fromFile(file).mkString
  }

  def main(args: Array[String]): Unit = {
    implicit def file2RichFile(file: File): RichFile = new RichFile(file) //自动导入

    //相当于使用 RichFile 来装饰 File，自动将 File 类型转换成 RichFile 并执行 RichFile 中的方法
    val file = new File("data.txt")
    println(file.read())
  }
}
