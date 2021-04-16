package edu.analysis.streaming

import com.google.gson.Gson
import edu.bean.Answer
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 从 kafka 的 edu 主题中消费数据
 * 实时统计分析
 * 1.统计 top10 热点题目
 * 2.统计 top10 答题活跃年级
 * 3.统计 top10 热点题目 + 所属科目
 * 4.统计 top10 题目得分最低学生
 * 输出到控制台/ mysql
 */
object StreamingAnalysis {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("edu").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = ses.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "edu")
      .load()

    import ses.implicits._
    val valueDS: Dataset[String] = df.selectExpr("CAST(value AS STRING)").as[String]
    val answerDS: Dataset[Answer] = valueDS.filter(StringUtils.isNoneBlank(_))
      .map(jsonStr => {
        val gson = new Gson()
        gson.fromJson(jsonStr, classOf[Answer]) // json -> 对象
      })

//    answerDS.writeStream  //测试
//      .format("console")
//      .outputMode(OutputMode.Append())
//      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/edu")
//      .start()
//      .awaitTermination()

    val result1: Dataset[Row] = answerDS.groupBy('question_id)
      .count()
      .orderBy('count.desc)
      .limit(10)
    val result2: Dataset[Row] = answerDS.groupBy('grade_id)
      .count()
      .orderBy('count.desc)
      .limit(10)
    import org.apache.spark.sql.functions._
    val result3: Dataset[Row] = answerDS.groupBy('question_id)
      .agg(
        first('subject_id) as "subject_id",
        count('question_id) as "counts"
      )
      .orderBy('counts.desc)
      .limit(10)
    val result4: Dataset[Row] = answerDS.groupBy('student_id)
      .agg(
        min('score) as "minscore",
        first('question_id)
      )
      .orderBy('minscore)
      .limit(10)

    result1.writeStream  //测试
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/result1")
      .start()
    result2.writeStream  //测试
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/result2")
      .start()
    result3.writeStream  //测试
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/result3")
      .start()
    result4.writeStream  //测试
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "hdfs://node1:8020/checkpoint/result4")
      .start()



    ses.streams.awaitAnyTermination()
    ses.close()
  }
}
