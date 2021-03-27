package kafka_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) {
        //生产者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092");
        props.put("acks", "all");   //级别
        props.put("retries", 1);    //重发次数
        props.put("batch.size", 16384); //批大小，到了 16k 发送
        props.put("linger.ms", 1);  //等待时间，到了 1ms 发送
        props.put("buffer.memory", 33554432);   // RecordAccumulator 缓冲区大小
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1");
//        props.put(ProducerConfig.ACKS_CONFIG, "all");

        //生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //发送数据（topic，value）
        for(int i=0; i<10; i++){
            producer.send(new ProducerRecord<>("first", "value--" + i));
        }

        producer.close();
    }
}
