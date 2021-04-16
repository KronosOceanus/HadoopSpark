package structured_source

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Demo01_Socket {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("structured_source").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = ses.readStream
      .format("socket") //从 socket 输入
      .option("host", "node1")  // nc -lk 9999
      .option("port", "9999")
      .load()

    import ses.implicits._
    val ds: Dataset[String] = df.as[String]
    val result: Dataset[Row] = ds.flatMap(_.split(" "))
      .groupBy('value)
      .count()
      .orderBy('count desc)

    result.writeStream
      .format("console")  //输出到控制台
      .outputMode("complete") //必须有聚合操作
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/socket")
      .start()
      .awaitTermination()

    ses.close()
  }
}
