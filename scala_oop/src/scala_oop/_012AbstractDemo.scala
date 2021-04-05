package scala_oop

object _012AbstractDemo {

  abstract class Shape{
    def area(): Double
  }

  class Square(var edge: Double) extends Shape {
    override def area(): Double = edge * edge
  }

  class Circle(var radius: Double) extends Shape {
    override def area(): Double = Math.PI * radius * radius
  }

  def main(args: Array[String]): Unit = {
    val square = new Square(2.0)
    val circle = new Circle(2.0)

    println(square.area())
    println(circle.area())

    val shape = new Shape {
      override def area(): Double = 2.0
    }
    println(shape.area())
  }
}
