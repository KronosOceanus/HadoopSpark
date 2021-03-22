package spark_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDDemo05_Agg_NoKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[Int] = sc.parallelize(1 to 10)

    //返回值不是 RDD，是 Action
    println(rdd1.sum)
    println(rdd1.reduce(_+_))
    println(rdd1.fold(1)(_+_))  //？
    println(rdd1.aggregate(1)(_+_, _+_))  //（初始值）（局部聚合，全局聚合）
  }
}
