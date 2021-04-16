package structured_operation

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 阻塞？？？
 */
object Demo04_Operation {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("structured_source").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = ses.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("subscribe", "first")
      .load()

    import ses.implicits._
    val wordsDS: Dataset[String] = df
      .selectExpr("CAST(value AS STRING)")
      .as[String].flatMap(_.split(" "))
    val result1: Dataset[Row] = wordsDS.groupBy('value)
      .count()
      .orderBy('count desc)

    wordsDS.createOrReplaceTempView("t_words")
    val sql: String = "select value,count(*) as counts " +
      "from t_words " +
      "group by value " +
      "order by counts desc"
    val result2: DataFrame = ses.sql(sql)

    val query1: StreamingQuery = result1.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/operation1")
      .start()
    val query2: StreamingQuery = result2.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/operation2")
      .start()

    ses.streams.awaitAnyTermination()

    ses.close()
  }


}
