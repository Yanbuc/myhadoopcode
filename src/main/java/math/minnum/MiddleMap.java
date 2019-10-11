package math.minnum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MiddleMap extends Mapper<LongWritable,Text, IntWritable, DoubleWritable> {
    private IntWritable userId=new IntWritable();
    private DoubleWritable rate=new DoubleWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String log=value.toString();
        String[] fields=log.split(",");
        if(fields[0].contains("user")|| fields.length< 4){
            return ;
        }
        userId.set(Integer.valueOf(fields[0]));
        rate.set(Double.valueOf(fields[2]));
        context.write(userId,rate);
    }
}
