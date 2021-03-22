package spark_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDDemo04_Repart {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[Int] = sc.parallelize(1 to 10)  // RDD 的内容
    println(rdd1.getNumPartitions)

    val rdd2: RDD[Int] = rdd1.repartition(7)
    println(rdd2.getNumPartitions)

    val rdd3: RDD[Int] = rdd2.coalesce(5) //默认只能减少分区，shuffle=true 才能增加分区（repartition 底层）
    println(rdd3.getNumPartitions)
  }
}
