package spark_mllib

import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ALSMovieDemo {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("movie").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    import ses.implicits._
    val fileDS: Dataset[String] = ses.read.textFile("input/u.data")
    val ratingDF: DataFrame = fileDS.map(line => {
      val arr: Array[String] = line.split("\t")
      (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
    }).toDF("userId", "movieId", "score")

    val Array(trainSet, testSet) = ratingDF.randomSplit(Array(0.8, 0.2))
    val als: ALS = new ALS()
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("score")
      .setRank(10)
      .setMaxIter(10)
      .setAlpha(1.0)  //学习率
    val model: ALSModel = als.fit(trainSet)
    val testResult: DataFrame = model.recommendForUserSubset(testSet , 5)
    val oneResult: DataFrame = model.recommendForUserSubset(
      sc.makeRDD(Array(471)).toDF("userId"), 5)  //给 471 用户推荐 5 部电影

    testResult.show(false)
    oneResult.show(false)

    ses.stop()
  }
}
