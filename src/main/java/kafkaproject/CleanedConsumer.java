package kafkaproject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class CleanedConsumer {
    private KafkaConsumer<String,String> consumer=null ;
    private  String topic;

    public CleanedConsumer(String topic) {
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
        properties.put("group.id", "cleaned");
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
        Calendar calendar=Calendar.getInstance();
        while (true){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String,String> consumerRecord : consumerRecords){
                String message=StringUtils.filterLog(consumerRecord.value(),token,fg);
                fields=message.split(token);
                if(fields.length<7){
                    continue;
                }
                String visitPage=StringUtils.getVisitPage(fields[5]);
                if(!visitPage.endsWith(".html")){
                    continue;
                }
                Date reqDate = StringUtils.str2Date(fields[2]);
                calendar.setTime(reqDate);
                String hostName=fields[0];
                String method=fields[3];
                String status=fields[4];
                int year=calendar.get(Calendar.YEAR);
                String mo="";
                int month=calendar.get(Calendar.MONTH)+1;
                if(month<10){
                    mo="0"+month;
                }else {
                    mo=""+month;
                }
                String da="";
                int day=calendar.get(Calendar.DAY_OF_MONTH);
                if(day<10){
                    da="0"+day;
                }else{
                    da=""+day;
                }
                String msg=hostName+"\t"+method+"\t"+status+"\t"+visitPage+"\t"+year+"\t"+mo+"\t"+da;
                String filePath="/busi/"+year+"/"+mo+"/"+da+"/"+hostName+".log";
                writ.write(filePath,msg);
            }
        }

    }

}
