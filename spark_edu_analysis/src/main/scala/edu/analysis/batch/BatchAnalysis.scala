package edu.analysis.batch

import edu.bean.AnswerWithRecommendations
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.Properties

/**
 * 离线分析
 * 1.统计 top50 热点题目，统计这些题目中每个科目包含的热点题目数
 * 2.找到 top20 热点题目对应的推荐题目，找到推荐题目对应的科目，并统计每个科目分别包含的推荐题目的条数
 */
object BatchAnalysis {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("edu").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    import ses.implicits._
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "java521....")
    val allInfoDF: Dataset[AnswerWithRecommendations] = ses.read.
      jdbc("jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
      "t_recommended",
      props)
      .as[AnswerWithRecommendations]

    allInfoDF.createOrReplaceTempView("t_answer")
    val sql1: String = "select subject_id,count(t_answer.question_id) as hot_question_count " +
      "from (select question_id,count(*) as frequency  " +
      "from t_answer " +
      "group by question_id " +
      "order by frequency desc " +
      "limit 50)t1 " +
      "join t_answer " +
      "on t1.question_id = t_answer.question_id " +
      "group by subject_id " +
      "order by hot_question_count desc"
    ses.sql(sql1).show()

    import org.apache.spark.sql.functions._
    val top50: Dataset[Row] = allInfoDF.groupBy('question_id)
      .agg(count('question_id) as "frequency")
      .orderBy('frequency.desc)
      .limit(50)
    val joinDF1: DataFrame = top50.join(allInfoDF, "question_id")
    val result1: DataFrame = joinDF1.groupBy('subject_id)
      .agg(count('question_id) as "hot_question_count")
      .orderBy('hot_question_count.desc)
    result1.printSchema()
    result1.show()

    val sql2: String = "select subject_id,count(*) as r_count " +
      "from (select t5.question_id, t_answer.subject_id " +
      "from (select question_id " +
      "from (select explode(split(recommendations,',')) as question_id " +
      "from (select t2.recommendations " +
      "from (select question_id,count(*) as frequency " +
      "from t_answer " +
      "group by question_id " +
      "order by frequency desc " +
      "limit 20)t1 " +
      "join (select question_id,first(recommendations) as recommendations " +
      "from t_answer " +
      "group by question_id)t2 " +
      "on t1.question_id = t2.question_id)t3)t4 " +
      "group by question_id)t5 " +
      "join t_answer " +
      "on t5.question_id = t_answer.question_id)t6 " +
      "group by subject_id " +
      "order by r_count desc"
    ses.sql(sql2).show()

    val top20: Dataset[Row] = allInfoDF.groupBy('question_id)
      .agg(count('question_id) as "frequency")
      .orderBy('frequency.desc)
      .limit(20)
    val joinDF2: DataFrame = top20.join(allInfoDF.dropDuplicates("question_id"),
      "question_id")
      .select( 'recommendations)
    val ridDS: Dataset[Row] = joinDF2.select(
      explode(split('recommendations, ",")) as "question_id")
      .dropDuplicates("question_id")
    val ridAndSidDS: Dataset[Row]  = ridDS.join(allInfoDF.dropDuplicates("question_id"),
      "question_id")
    val result2: DataFrame = ridAndSidDS.groupBy('subject_id)
      .agg(count('*) as "r_count")
      .orderBy('r_count.desc)
    result2.printSchema()
    result2.show()

    ses.stop()
  }
}
