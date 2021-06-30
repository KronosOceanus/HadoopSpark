package spark_mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

object SVMWithSGDExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/libsvm_iris/iris_training")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // 损失，roc 曲线下面积
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println(s"Area under ROC = $auROC")

    // Save and load model
//    model.save(sc, "model/scalaSVMWithSGDModel")
    val sameModel = SVMModel.load(sc, "model/scalaSVMWithSGDModel")

    println("**************************************")
    println("权重和偏置")
    println("**************************************")
    sameModel.weights.foreachActive((index, w) => println(s"index : ${index}\t weight : ${w}"))
    println(s"intercept : ${sameModel.intercept}")
    println("**************************************")
    println("测试集预测分数和标签")
    println("**************************************")
    scoreAndLabels.foreach(println)

    sc.stop()
  }
}