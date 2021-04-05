package scala_implicit

import java.io.File

import scala.io.Source

object _01ImplicitDemo {

  //传入要隐式转换的参数
  class RichFile(file: File){
    def read(): String = Source.fromFile(file).mkString
  }

  object Imp{
    //扩展增强，该方法自动调用
    implicit def file2RichFile(file: File): RichFile = new RichFile(file) //隐式转换
  }

  def main(args: Array[String]): Unit = {
    import Imp.file2RichFile  //手动导入 implicit 修饰的方法

    //相当于使用 RichFile 来装饰 File，自动将 File 类型转换成 RichFile 并执行 RichFile 中的方法
    val file = new File("data.txt")
    println(file.read())
  }
}
