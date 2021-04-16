package kafka_etl

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 从 iotTopic 消费数据
 * 使用  structured streaming 进行数据分析
 * 统计 signal > 30 的 deviceType 数量，以及该 deviceType 的平均 signal
 * 输出到控制台
 */
object Kafka_IOT {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("structured_source").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = ses.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "iotTopic")
      .load()

    import ses.implicits._
    val valueDS: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    //解析 json，可以使用 fastJson / sparkSQL 内置函数
    import org.apache.spark.sql.functions._
    val schemaDF: DataFrame = valueDS.filter(StringUtils.isNoneBlank(_))
      .select(
        get_json_object($"value", "$.device").as("device_id"),
        get_json_object($"value", "$.deviceType").as("device_type"),
        get_json_object($"value", "$.signal").cast(DoubleType).as("signal") //类型转换
      )

    schemaDF.createOrReplaceTempView("t_iot")
    val sql: String = "select device_type,count(*) as counts,avg(signal) as avg_signal " +
      "from t_iot " +
      "where signal > 30 " +
      "group by device_type"
    val result1: DataFrame = ses.sql(sql)

    result1.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/kafka_iot")
      .start()
      .awaitTermination()

    val result2: DataFrame = schemaDF.filter('signal > 30)
      .groupBy('device_type)
      .agg( //多次聚合
        count('device_id) as "counts",
        avg('signal) as "avg_signal"
      )

    ses.close()
  }

}
