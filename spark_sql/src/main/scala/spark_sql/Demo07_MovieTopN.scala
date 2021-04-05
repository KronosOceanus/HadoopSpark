package spark_sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 获取电影评分 top 10，并且要求评分次数 > 200
 */
object Demo07_MovieTopN {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("sql").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val ds: Dataset[String] = ses.read.textFile("input/movie.txt")
    import ses.implicits._
    val scoreDS: Dataset[(String, Int)] = ds.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(1), arr(2).toInt)
    })
    scoreDS.printSchema()
    scoreDS.show()
    println("---------------------")

    //增加列名
    val scoreDF: DataFrame = scoreDS.toDF("movieId", "score")

    scoreDF.createOrReplaceTempView("t_score")
    val sql: String =
      "select movieId, avg(score) as avgscore, count(1) as counts " + //需要的数据
      "from t_score " +
      "group by movieId " + //要计算 avg，所以先聚合
      "having counts > 200 " +  //聚合之后统计聚合的数量结果
      "order by avgscore desc " +
      "limit 10"
    ses.sql(sql).show()

    import org.apache.spark.sql.functions._
    scoreDF.groupBy('movieId).
      agg(
        avg('score) as "avgscore",
        count('movieId) as "counts"
      ).
      filter('counts > 200).
      orderBy('avgscore desc).
      limit(10).
      show()
  }
}
