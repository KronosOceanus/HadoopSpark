package spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDDemo07_Join {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val empRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoliu"))
    )
    val deptRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "销售部"), (1002, "技术部"))
    )

    val result1: RDD[(Int, (String, String))] = empRDD.join(deptRDD); //默认按照 key join
    val result2: RDD[(Int, (String, Option[String]))] = empRDD.leftOuterJoin(deptRDD);
    val result3: RDD[(Int, (Option[String], String))] = empRDD.rightOuterJoin(deptRDD);

    result1.foreach(println)
    println("----------------------------")
    result2.foreach(println)
    println("----------------------------")
    result3.foreach(println)

  }
}
