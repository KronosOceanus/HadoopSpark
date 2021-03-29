package kafka_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
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

//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
//        props.put(ProducerConfig.ACKS_CONFIG, "all");

        //生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //同步发送数据（topic，value），用 sender 线程阻塞 main 线程，在单分区时候可以保证有序
        for(int i=0; i<10; i++){
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>("first", "key1","value--" + i));
            RecordMetadata recordMetadata = future.get();   //调用 Future 的 get 方法会阻塞前面的线程
            System.out.println(recordMetadata.partition() + "----" + recordMetadata.offset());
        }

        producer.close();
    }
}
