package spark_hive

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Demo09_Hive {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("sql").master("local[*]").
      config("spark.sql.shuffle.partitions", "4").
      config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse").
      config("hive.metastore.uris", "thrift://node3:9083").
      enableHiveSupport().
      getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    ses.sql("use mytest")
    ses.sql("select uploader,videoid,views \nfrom (select uploader,videoid,views,rank() over(partition by uploader order by views desc) rk \nfrom (select t1.uploader,videoid,views \nfrom (select uploader \nfrom gulivideo_user_orc \norder by videos desc   \nlimit 10)t1 \njoin gulivideo_orc g \non t1.uploader = g.uploader)t2)t3 \nwhere rk <= 20;").show()

    ses.stop()
  }
}
