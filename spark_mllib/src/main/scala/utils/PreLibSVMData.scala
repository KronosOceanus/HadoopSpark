package utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将 iris 数据集格式化为 libsvm 形式
 */
object PreLibSVMData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PreLibSVMData").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val data = sc.textFile("data/iris/iris_training.csv")
    val libsvm_data = data
      .map(_.split(","))
      .filter(p => p(4).toDouble != 2.0) //满足条件保留
      .map(p => s"${p(4)} 1:${p(0).toDouble} 2:${p(1).toDouble} 3:${p(2).toDouble} 4:${p(3).toDouble}")

    libsvm_data.saveAsTextFile("data/libsvm_iris/iris_training")

    sc.stop()
  }
}
