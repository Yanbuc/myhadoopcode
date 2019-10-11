package datadistinct;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DisMap extends Mapper<LongWritable,Text, Text, IntWritable> {
    private Text date=new Text();
    private IntWritable num=new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data=value.toString();
        String[] fields=data.split("\\s+");
        date.set(fields[0]);
        num.set(Integer.valueOf(fields[1]));
        context.write(date,num);
    }
}
