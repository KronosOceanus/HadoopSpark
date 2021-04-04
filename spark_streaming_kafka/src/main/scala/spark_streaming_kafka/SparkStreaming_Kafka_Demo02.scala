package spark_streaming_kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 手动提交偏移量
 */
object SparkStreaming_Kafka_Demo02 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("kafka").setMaster("local[*]") //必须是 local[*]
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092",  // kafka 集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark",  //消费者组
      "auto.offset.reset" -> "latest",  //如果有 offset 从 offset 开始消费，否则从最新记录开始消费
      "auto.commit.interval.ms" -> "1000",  //自动提交时间间隔
      "enable.auto.commit" -> (false: java.lang.Boolean)  //关闭自动提交
    )

    val topics = Array("first", "second")  //订阅的主题，还可以用正则订阅
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String]( //直连 kafka
      ssc,
      PreferConsistent, //位置策略，有几个分区 spark 就开几个 exector 消费数据
      Subscribe[String, String](topics, kafkaParams)
    )

    //消费一批提交一次 offset（即一个 RDD）
    kafkaDS.foreachRDD(rdd => {
      if(! rdd.isEmpty()){
        rdd.foreach(record => {
          val topic: String = record.topic()
          val partition: Int = record.partition()
          val offset: Long = record.offset()
          val key: String = record.key()
          val value: String = record.value()
          println(s"topic: ${topic}, part: ${partition}, offset: ${offset}, key: ${key}, value: ${value}")
        })
        //从 rdd 获取每个分区的 offset，并异步提交
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //类型转换
        kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println("----当前批次的 offset 已手动提交----")
      }
    })

//    /usr/local/kafka/bin/kafka-console-producer.sh --topic first --broker-list node1:9092
//    /usr/local/kafka/bin/kafka-console-producer.sh --topic second --broker-list node1:9092

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
