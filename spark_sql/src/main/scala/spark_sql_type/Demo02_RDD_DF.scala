package spark_sql_type

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 将 RDD 转为 DataFrame
 */
object Demo02_RDD_DF {

  case class Person(id: Int, name: String, age: Int)  //常用

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
    val personRDD2: RDD[(Int, String, Int)] = lines.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(0).toInt, arr(1), arr(2).toInt)
    })

    import ses.implicits._  //隐式转换，为 RDD 添加 toDF() 方法
    val personDF: DataFrame = personRDD.toDF()
    val personDF2: DataFrame = personRDD2.toDF("id", "name", "age")

    personDF.printSchema()
    personDF.show()
    println("----------------------")
    personDF2.printSchema()
    personDF2.show()

    ses.stop()
  }
}
