package edu.analysis.streaming

import com.google.gson.Gson
import edu.bean.Answer
import edu.utils.RedisUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util.Properties

/**
 * SparkStreaming
 * ALS 实时推荐易错题
 *
 * 从 Kafka 的 edu 主题消费数据
 * 从 redis 中获取推荐模型的路径，加载 ALS 模型
 * 向学生推荐易错题
 */
object StreamingRecommend {

  def main(args: Array[String]): Unit = {
    val ses: SparkSession = SparkSession.
      builder().appName("edu").master("local[*]").getOrCreate()
    val sc: SparkContext = ses.sparkContext
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092",  // kafka 集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "edu",  //消费者组
      "auto.offset.reset" -> "latest",  //如果有 offset 从 offset 开始消费，否则从最新记录开始消费
      "auto.commit.interval.ms" -> "1000",  //自动提交时间间隔
      "enable.auto.commit" -> (true: java.lang.Boolean)  //自动提交
    )
    val topic: Array[String] = Array("edu")
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
    )
    val valueDS: DStream[String] = kafkaDS.map(_.value())

    import ses.implicits._
    import org.apache.spark.sql.functions._
    valueDS.foreachRDD(rdd => { //微批处理（5s 一次）
      if(! rdd.isEmpty()){
        val jedis: Jedis = RedisUtil.pool.getResource
        val path: String = jedis.hget("als_model", "recommended_question_id")

        val model: ALSModel = ALSModel.load(path)
        val answerDF: DataFrame = rdd.coalesce(1).map(jsonStr => {
          val gson = new Gson()
          gson.fromJson(jsonStr, classOf[Answer])
        }).toDF()
        val id2int = udf((student_id: String) => {  //自定义 udf
          student_id.split("_")(1).toInt
        })
        val studentIdDF: DataFrame = answerDF.select(id2int('student_id) as 'user) //类型转换
        val recommendDF: DataFrame = model.recommendForUserSubset(studentIdDF, 10) //推荐
        recommendDF.printSchema()
        recommendDF.show(false)

        //处理推荐结果，"student_id":"question_id1,question_id2,question_id3,..."
        val recommendResult: DataFrame = recommendDF.as[(Int, Array[(Int, Float)])]
          .map(t => {
            val studentId: String = s"学生ID_${t._1}"
            val questionIds: String = t._2.map("题目ID_" + _._1).mkString(",")
            (studentId,questionIds)
          }).toDF("student_id", "recommendations")
        //还要保留学生做题信息（就是根据该信息推荐出的 10 结果）
        val allInfoDF: DataFrame = answerDF.join(recommendResult, "student_id")
        //输出到 mysql
        if(! allInfoDF.isEmpty){
          val props: Properties = new Properties()
          props.setProperty("user", "root")
          props.setProperty("password", "java521....")
          allInfoDF.coalesce(1)
            .write
            .mode(SaveMode.Append)
            .jdbc("jdbc:mysql://node3:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
              "t_recommended",
              props)
          println("推荐数据已保存到mysql")
        }


        jedis.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
