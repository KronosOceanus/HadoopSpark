package spark_3new

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object DynamicPartition {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("dynamic_partition").master("local[*]")
      .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false") //动态分区，默认 true
      .getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    import ses.implicits._
    //创建表
    ses.range(10000)  //随机生成 1~10000 的数字
      .select('id, 'id as "k")
      .write
      .partitionBy("k")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("t1")
    ses.range(100)
      .select('id, 'id as "k")
      .write
      .partitionBy("k")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("t2")

    val sql: String = "select * from t1 " +
      "join t2 " +
      "on t1.k = t2.k " +
      "and t2.k < 2"
    ses.sql(sql).explain()
    ses.sql(sql).show()

    Thread.sleep(Long.MaxValue) //等待查看 webUI 界面

    ses.stop()
  }
}
