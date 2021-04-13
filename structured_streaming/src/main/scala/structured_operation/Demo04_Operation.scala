package structured_operation

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 *
 */
object Demo04_Operation {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("structured_source").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val csvSchema: StructType = new StructType().add("value", StringType, nullable = true)

    val df: DataFrame = ses.readStream
      .option("sep", ",")
      .option("header", "false")
      .schema(csvSchema)
      .format("csv")
      .load("input")  //动态监控该文件夹下的文件

    import ses.implicits._
    val ds: Dataset[String] = df.as[String]
    val result1: Dataset[Row] = ds.flatMap(_.split(","))
      .groupBy('value)
      .count()
      .orderBy('count desc)

    ds.createOrReplaceTempView("t_words")
    val sql: String = "select count(*) as counts " +
      "from t_words " +
      "group by value " +
      "order by counts desc"
    val result2: DataFrame = ses.sql(sql)

    result2.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }


}
