package structured_source

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Demo02_Rate {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("structured_source").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = ses.readStream
      .format("rate")
      .option("rowsPerSecond", "10")  //每 s 生成数据
      .option("rampUpTime", "0s") //生成数据间隔
      .option("numPartitions", "2") //分区
      .load()

    df.writeStream
      .format("console")  //输出到控制台
      .outputMode("append")
      .option("truncate", "false")  //不截断列
      .start()
      .awaitTermination()

    ses.close()
  }
}
