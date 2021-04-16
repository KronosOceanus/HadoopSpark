package structured_window

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

/**
 * 解决数据延迟到达问题（有延迟时间限制）
 */
object Demo10_EventTime_WaterMark {

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
    val wordDF: DataFrame = df.as[String]
      .filter(StringUtils.isNoneBlank(_))
      .map(line => {  //数据 2019-10-12 09:00:02,cat
        val arr: Array[String] = line.trim.split(",")
        val timestampStr: String = arr(0)
        val word: String = arr(1)
        (Timestamp.valueOf(timestampStr), word)
      }).toDF("timestamp", "word")

    import org.apache.spark.sql.functions._
    val windowCounts = wordDF
      .withWatermark("timestamp", "10 seconds") //允许 10s 延迟
      .groupBy(
        window($"timestamp",  //窗口计算的字段（时间戳）
          "10 seconds", "5 seconds"), //每 5s 计算前 10s 数据
        $"word"
      )
      .count()

    windowCounts.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .option("truncate", "false")
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/window")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()

    ses.close()
  }
//2019-10-10 12:00:07,dog
//2019-10-10 12:00:08,owl
//
//2019-10-10 12:00:14,dog
//2019-10-10 12:00:09,cat
//
//2019-10-10 12:00:15,cat
//2019-10-10 12:00:08,dog
//2019-10-10 12:00:13,owl
//2019-10-10 12:00:21,owl
//
//2019-10-10 12:00:04,donkey
//2019-10-10 12:00:17,owl
}
