package spark_rdd_high

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDDDemo09_Cache {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("cache").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("input/wordcount.txt")
    val result: RDD[(String, Int)] =lines.filter(StringUtils.isNotBlank).
      flatMap(_.split(",")).
      map((_,1)).
      reduceByKey(_+_)

//    result.cache()
//    result.persist()  // cache 的底层
    //对于复杂的 RDD，先缓存，再 checkpoint
    result.persist(StorageLevel.MEMORY_AND_DISK)  //选择缓存的等级（保留了 RDD 的依赖关系，不能保证数据安全）
    sc.setCheckpointDir("hdfs://node1:8020/checkpoint")
    result.checkpoint() //不保留 RDD 的依赖关系

    val top1 = result.sortBy(_._2, ascending = false)
    top1.foreach(println)

    result.unpersist()  //清空缓存
  }

}
