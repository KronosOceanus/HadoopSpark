package spark_sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 查询
 */
object Demo05_Query {

  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("sql").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("input/person.txt")
    val personRDD: RDD[Person] = lines.map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })

    import ses.implicits._
    val personDF: DataFrame = personRDD.toDF()

    // sql
    personDF.createOrReplaceTempView("t_person")  //创建或替换视图，global 的视图在 ses 内都可以用
    ses.sql("select * from t_person").show()
    println("----------------------")
    ses.sql("select count(*) from t_person where age > 15").show()
    println("----------------------")

    // dsl
    personDF.select("*").show()
    println("----------------------")
    personDF.select($"age", $"age"+1).show()  // $ 把 String 变成 Column
    println("----------------------")
    personDF.select('age, 'age+1).show()
    println("----------------------")
    personDF.filter("age > 15").show()
    println("----------------------")
    val count: Long = personDF.where("age > 15").count()
    println(count)
    println("----------------------")
    personDF.groupBy('age).count().show()
    println("----------------------")
    personDF.filter('name === "p3").show()
    println("----------------------")
    personDF.filter('name =!= "p3").show()
    println("----------------------")

    ses.stop()
  }
}
