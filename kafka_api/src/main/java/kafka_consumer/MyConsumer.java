package kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //手动提交 offset，不提交，则在消费者重启后，继续从提交之前的 offset 开始读取
        //存在内存中未写入，但没必要多次写入，所以要手动提交
        //通过生产者发送消息之后，重复启动消费者
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);  //启动自动提交 offset
//        //自动提交 offset 延时。延时短容易丢失数据，延时长容易重复数据
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");  //消费者组
        //当消费者未初始化 offset，或 offset 指定的数据已删除，从头读取，通过换消费者组可以重新消费
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("first", "second"));   //订阅主题

        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100); //批量拉去（时间间隔）
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "----" +
                        consumerRecord.value());
            }
        }
    }
}
