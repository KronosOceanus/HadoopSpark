package structured_sink

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Demo05_Memory {

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
      .format("memory")
      .queryName("t_result")  //将结果放在一张表中
      .outputMode("complete")
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/sink")
      .start()

    while(true){
      ses.sql("select * from t_result").show()
      Thread.sleep(5000)
    }

    ses.close()
  }
}
