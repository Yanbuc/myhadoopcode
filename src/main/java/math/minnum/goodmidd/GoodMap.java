package math.minnum.goodmidd;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GoodMap extends Mapper<LongWritable, Text, IntWritable, GoodKey> {
    private IntWritable userId=new IntWritable();
    private GoodKey goodKey=new GoodKey();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String log=value.toString();
        if(log.contains("user")){
            return ;
        }
        String[] fields=log.split(",");
        userId.set(Integer.valueOf(fields[0]));
        goodKey.setRate(Double.valueOf(fields[2]));
        goodKey.setNum(1);
        context.write(userId,goodKey);
    }
}
