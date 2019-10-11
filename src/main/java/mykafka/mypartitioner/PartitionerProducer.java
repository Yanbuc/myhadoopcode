package mykafka.mypartitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class PartitionerProducer {
    private static final String[] PHONE_NUMS = new String[]{
            "10000", "10000", "11111", "13700000003", "13700000004",
            "10000", "15500000006", "11111", "15500000008",
            "17600000009", "10000", "17600000011"
    };

    public static void main(String[] args) throws Exception {

        /*
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.240.10:9092,192.168.240.11:9092,192.168.240.12:9092");
        // 设置分区器
       // props.put("partitioner.class", "com.bonc.rdpe.kafka110.partitioner.PhonenumPartitioner");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        int count = 0;
        int length = PHONE_NUMS.length;

        while(count < 10) {
            Random rand = new Random();
            String phoneNum = PHONE_NUMS[rand.nextInt(length)];
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("mm", phoneNum, phoneNum);
            RecordMetadata metadata = producer.send(record).get();
            String result = "phonenum [" + record.value() + "] has been sent to partition " + metadata.partition();
            System.out.println(result);
            count++;
            System.out.println("hello ");
        }
        producer.close();
        */
        int events = 100;
        Properties props = new Properties();
        //集群地址，多个服务器用"，"分隔
        props.put("bootstrap.servers", "192.168.249.10:9092");
        //key、value的序列化，此处以字符串为例，使用kafka已有的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("partitioner.class", "com.kafka.demo.Partitioner");//分区操作，此处未写
        props.put("request.required.acks", "1");
        //创建生产者
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < events; i++){
            long runtime = new Date().getTime();
            String ip = "192.168.1." + i;
            String msg = runtime + "时间的模拟ip：" + ip;
            //写入名为"test-partition-1"的topic
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("mm", "key-"+i, msg);
            producer.send(producerRecord);
            System.out.println("写入test-partition-1：" + msg);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();


    }
}
