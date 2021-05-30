package spark_mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KMeansExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KMeansExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 训练数据
    val data = sc.textFile("data/iris/iris_training.csv")
    val parsedData = data
      .map(_.split(","))
      .map(p => Vectors.dense(p(0).toDouble,p(1).toDouble,p(2).toDouble,p(3).toDouble))

    // 超参数，训练
    val numClusters = 3
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // 评估
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // 保存，加载模型
//    clusters.save(sc, "model/IrisKMeansModel")
    val model = KMeansModel.load(sc, "model/IrisKMeansModel")

    println("***************************************")
    println("聚类中心")
    println("****************************************")
    model.clusterCenters.foreach(println)

    // 测试数据
    val testData = sc.textFile("data/iris/iris_test.csv")
    val parsedTestData = testData
      .map(_.split(","))
      .map(p => Vectors.dense(p(0).toDouble,p(1).toDouble,p(2).toDouble,p(3).toDouble))
    println("****************************************")
    println("预测")
    println("****************************************")
    parsedTestData.map(v => v.toString + " belong to cluster " + model.predict(v))
      .foreach(println)

    sc.stop()
  }
}