package structured_source

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Demo09_Kafka {

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
    val ds: Dataset[(String, String)] = df.
      selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") //序列化器
      .as[(String, String)]
    val result: Dataset[Row] = ds.flatMap(_._2.split(" "))
      .groupBy('value)
      .count()
      .orderBy('count desc)

    result.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/kafka")
      .start()
      .awaitTermination()

    ses.close()
  }
}
