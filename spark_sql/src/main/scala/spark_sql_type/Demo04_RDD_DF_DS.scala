package spark_sql_type

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import spark_sql_type.Demo02_RDD_DF.Person

object Demo04_RDD_DF_DS {

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

    // RDD -> DF/DS
    import ses.implicits._
    val personDF: DataFrame = personRDD.toDF()
    val personDS: Dataset[Person] = personRDD.toDS()

    // DF/DS -> RDD
    val personRDD2: RDD[Row] = personDF.rdd  //默认泛型 Row
    val personRDD3: RDD[Person] = personDS.rdd

    // DF <-> DS
    val personDS2: Dataset[Person] = personDF.as[Person]  //加上泛型
    val personDF2: DataFrame = personDS.toDF()

    personRDD2.foreach(println)
    println("----------------------")
    personDS2.show()
    println("----------------------")
    personDF2.show()

    ses.stop()
  }
}
