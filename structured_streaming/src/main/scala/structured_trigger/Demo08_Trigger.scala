package structured_trigger

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Demo08_Trigger {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("structured_source").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = ses.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", "9999")
      .load()

    import ses.implicits._
    val ds: Dataset[String] = df.as[String]
    val result: Dataset[Row] = ds.flatMap(_.split(" "))
      .groupBy('value)
      .count()
      .orderBy('count desc)

    result.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("10 seconds")) // 5s 执行一次未批处理（默认 0s）
//      .trigger(Trigger.Once())  //只处理一次
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/trigger")
      .start()
      .awaitTermination()

    ses.close()
  }
}
