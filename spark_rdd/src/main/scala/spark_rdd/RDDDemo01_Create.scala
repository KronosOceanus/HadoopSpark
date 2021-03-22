package spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 五大属性：
 * ---分区，默认 hdfs block < 2 时分区数为 2，block > 2 时与 block 数相同
 * ---对每个分区操作
 * ---RDD 依赖
 * ---对于 KV 类型的 RDD 使用分区器（如 Hash）
 * ---就近计算
 * 分区原则：
 * 分区数尽量等于及群众 CPU core 数，充分利用 CPU 计算资源，实际中一般并行度设置为 CPU core 数的 2~3 倍
 */
object RDDDemo01_Create {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[Int] = sc.parallelize(1 to 10)  // RDD 的内容
    val rdd2: RDD[Int] = sc.parallelize(1 to 10, 3) //最小分区数

    val rdd3: RDD[Int] = sc.makeRDD(1 to 10)
    val rdd4: RDD[Int] = sc.makeRDD(1 to 10, 4)

    val rdd5: RDD[String] = sc.textFile("input/wordcount.txt")
    val rdd6: RDD[String] = sc.textFile("input/wordcount.txt", 3)
    val rdd7: RDD[String] = sc.textFile("input")
    val rdd8: RDD[String] = sc.textFile("input", 3)
    //（文件名，行数据）
    val rdd9: RDD[(String, String)] = sc.wholeTextFiles("input")  // 1，读小文件，使用 CPU core 尽可能少
    val rdd10: RDD[(String, String)] = sc.wholeTextFiles("input", 3)

    println(rdd1.getNumPartitions)
    println(rdd2.getNumPartitions)
    println(rdd3.getNumPartitions)
    println(rdd4.getNumPartitions)
    println(rdd5.getNumPartitions)
    println(rdd6.getNumPartitions)
    println(rdd7.getNumPartitions)
    println(rdd8.getNumPartitions)
    println(rdd9.getNumPartitions)
    println(rdd10.getNumPartitions)
  }
}
