package spark_sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 两种方式实现 wordcount
 */
object Demo06_WordCount {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("sql").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = ses.read.text("input/wordcount.txt")
    val ds: Dataset[String] = ses.read.textFile("input/wordcount.txt")

    import ses.implicits._
//    df.flatMap(_.split(" ")) //没有泛型，不能直接使用 split
    val words: Dataset[String] = ds.flatMap(_.split(","))  //隐式转换增强
    words.createOrReplaceTempView("t_words")
    words.show()
    println("----------------------")
    val sql: String = "select value,count(*) as counts " +
      "from t_words " +
      "group by value " +
      "order by counts desc"
    ses.sql(sql).show()
    println("----------------------")

    words.groupBy('value).
      count().
      sort('count desc).  //也可以改为 orderBy
      show()

    ses.stop()
  }
}
