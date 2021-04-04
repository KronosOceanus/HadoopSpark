package kafka_consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 自定义 offset，存储在 mysql 中用事务维护，最安全
 * 1.消费者维护一个 map，保存自己当前消费的 offset
 * 2.消费者消费信息后，将 offset 提交到自定义存储
 * 3.Rebalance 时消费者拉取 offset，避免重复消费
 */
public class MyCommitConsumer {

    private static final Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); //关闭自动提交
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("first", "second"), new ConsumerRebalanceListener() {
            //分区重新分配之后（增加或减少消费者数）触发
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //提交当前所有消费者的 offset 到自定义存储
                commitOffset(currentOffset);
            }
            //上个方法之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                for (TopicPartition partition : collection){
                    //清空消费者内存中的 offset，重新读取自定义存储中的 offset，定位到最近提交的 offset（对于每个分区）
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });

        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100); //批量拉去（时间间隔）
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "----" +
                        consumerRecord.value());
            }
            consumer.commitAsync();  //异步提交
        }
    }

    //获取某分区最新提交的 offset
    private static long getOffset(TopicPartition partition){
        return 0;
    }
    //提交该消费者所有分区的 offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset){

    }
}
