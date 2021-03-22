package spark_rdd

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDDemo03_Part {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("input/wordcount.txt")
    val result: RDD[(String, Int)] = lines.filter(StringUtils.isNotBlank).
      flatMap(_.split(",")).
      mapPartitions(iter => //作用于每个分区
        iter.map((_, 1))).  //作用于分区中的每条数据，有几个分区就开启几次连接
      reduceByKey(_+_)

    result.foreachPartition(iter =>
      iter.foreach(println))
  }
}
