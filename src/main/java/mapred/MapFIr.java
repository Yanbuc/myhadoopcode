package mapred;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.InetAddress;

// 这是一个简单的Map类 其实接下来的任务可以去思考下 需要处理什么类型的数据 数据的分布是具有什么样的特点的？
public class MapFIr extends Mapper<LongWritable, Text,Text,IntWritable> {
    int MISSING=9999;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InetAddress a=InetAddress.getLocalHost();
        System.out.println("hostaddress ：" + a.getHostAddress());

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String val=value.toString();
        String year=val.substring(15,19);
        int air;
        if(val.charAt(17)=='+'){
            air= Integer.valueOf(val.substring(88,92));
        }else{
            air=Integer.valueOf(val.substring(87,92));
        }
        String quality=val.substring(92,93);
        if(air !=MISSING && quality.matches("[01459]")){
            context.write(new Text(year),new IntWritable(air));
        }
    }
}
