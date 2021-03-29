package kafka_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PartitionerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //自定义分区
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                "kafka_partitioner.MyPartitioner");


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for(int i=0; i<10; i++){
            producer.send(new ProducerRecord<>("second", "key2","value--" + i),
                    (recordMetadata, e) -> {
                        if (e == null){
                            System.out.println(recordMetadata.partition() + "----" + recordMetadata.offset());
                        }
                    });
        }

        producer.close();
    }
}
