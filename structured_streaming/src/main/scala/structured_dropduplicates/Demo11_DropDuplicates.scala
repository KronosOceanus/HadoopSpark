package structured_dropduplicates

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 流去重和批去重 api 一样
 * 但是底层维护了状态
 */
object Demo11_DropDuplicates {

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
    val valueDS: Dataset[String] = df.as[String]
    val schemaDF: DataFrame = valueDS.filter(StringUtils.isNoneBlank(_))
      .select(
        get_json_object($"value", "$.eventTime").as("event_time"),
        get_json_object($"value", "$.eventType").as("event_type"),
        get_json_object($"value", "$.userID").as("user_id")
      )

    val result: Dataset[Row] = schemaDF
      .dropDuplicates("user_id", "event_time", "event_type") //去重
      .groupBy("user_id")
      .count()

    result.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/drop")
      .start()
      .awaitTermination()

    ses.stop()
  }
//{"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
//{"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
//{"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
//{"eventTime": "2016-01-10 10:01:50","eventType": "slide","userID":"1"}
//{"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
//{"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
//{"eventTime": "2016-01-10 10:01:50","eventType": "slide","userID":"1"}
}
