package structured_sink

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * 自定义输出
 */
object Demo06_ForeachBatch {

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
      .foreachBatch((ds: Dataset[Row], batchId: Long) => {
        //输出到控制台
        println(s"------${batchId}------")
        ds.show()
        //输出到 mysql
        ds.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("jdbc")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("url", "jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
          .option("user", "root")
          .option("password", "java521....")
          .option("dbtable", "mytest.struct_words")
          .save()
      })
      .outputMode("complete")
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/jdbc_sink")
      .start()
      .awaitTermination()

    ses.close()
  }
}
