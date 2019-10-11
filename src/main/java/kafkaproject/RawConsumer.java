package kafkaproject;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class RawConsumer {
    private   KafkaConsumer<String,String> consumer=null ;
    private  String topic;
    private String group="";

    public RawConsumer(String topic) {
        this.topic = topic;
        getKafkaConsumer();
    }

    // 获得kafakConsumer
    public  void  getKafkaConsumer(){
        Properties properties=new Properties();
        // 因为是消费者 所以连接的地址是 zookeeper的地址
        // 主要是把端口弄错了 所以就不对了 记住 要使用的端口是9092端口 这样才能够进行消费
        properties.put("bootstrap.servers","192.168.249.10:9092,192.168.249.11:9092,192.168.249.12:9092");
         // 设置消费者组
        properties.put("group.id", "raw");
        group="raw";
        properties.put("auto.offset.reset", "latest");
      //  properties.put("zookeeper.session.timeout.ms", "500");
       // properties.put("zookeeper.sync.time.ms", "250");
       // properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("enable.auto.commit", "true");
        // 为什么要设置key的解析器， 因数据是以二进制的形式存放在文本文件之中，
        //指定特定的序列化的方式。
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer=new KafkaConsumer<String, String>(properties);
    }

    // 对指定的主题进行处理
    public  void processTag(){
        // 订阅相关的主题
        consumer.subscribe(Arrays.asList(topic));
        String token="\\|\\|\\|";
        String fg="|||";
        String[] fields;
        HdfsWrit writ=new HdfsWrit();
        FSDataOutputStream out =null;
        String prePath="";
         while (true){
             ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
             for(ConsumerRecord<String,String> consumerRecord : consumerRecords){
                String message=StringUtils.filterLog(consumerRecord.value(),token,fg);
                fields=message.split(token);
                String filePath="/busi/"+fields[2].split(" ")[0]+"_"+fields[0]+".log";
                System.out.println("group is: "+group+"  partition is "+consumerRecord.partition()+" is :");
                writ.write(filePath,message);
             }
         }
    }

}
