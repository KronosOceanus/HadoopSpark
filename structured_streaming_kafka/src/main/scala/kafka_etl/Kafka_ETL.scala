package kafka_etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 从 stationTopic 消费数据
 * 使用 structured streaming 进行 ETL（过滤含有 success 的数据）
 * 将数据写入 etlTopic
 */
object Kafka_ETL {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("structured_source").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = ses.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "stationTopic")
      .load()

    import ses.implicits._
    val valueDS: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    val etlResult: Dataset[String] = valueDS.filter(! _.contains("success"))

    etlResult.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("topic", "etlTopic")
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/kafka_etl")
      .start()
      .awaitTermination()

    ses.stop()
  }

}
