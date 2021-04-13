package spark_sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 使用 udf 将数据转化为大写
 */
object Demo08_UDF {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("sql").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val ds: Dataset[String] = ses.read.textFile("input/udf.txt")

    ds.printSchema()
    ds.show()
    println("---------------------")

    ds.createOrReplaceTempView("t_word");
    ses.udf.register("small2big", (value: String) => {
      value.toUpperCase()
    })
    val sql: String = "select value, small2big(value) as bigValue " +
      "from t_word";
    ses.sql(sql).show()
    println("---------------------")

    import ses.implicits._
    import org.apache.spark.sql.functions._
    val small2big: UserDefinedFunction = udf((value: String) => {
      value.toUpperCase()
    })
    ds.select('value, small2big('value)).show()


    ses.stop()
  }
}
