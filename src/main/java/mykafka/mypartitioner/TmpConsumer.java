package mykafka.mypartitioner;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class TmpConsumer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.249.10:9092");
        props.put("group.id", "logGroup");
        // 自动确认设置
         props.put("enable.auto.commit", "true");
       // props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String,String> consu = new KafkaConsumer<String,String>(props);
        Collection<String> topics = Arrays.asList("mm");
        //消费者订阅topic
        consu.subscribe(topics);
        ConsumerRecords<String,String> consumerRecords = null;
        while(true){
            //接下来就要从topic中拉去数据
            consumerRecords = consu.poll(1000);
            //遍历每一条记录
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
                long offset = consumerRecord.offset();
                int partition = consumerRecord.partition();
                Object key = consumerRecord.key();
                Object value = consumerRecord.value();
                System.out.println(offset+" "+partition+" "+key+" "+value);
            }
        }
    }

}
