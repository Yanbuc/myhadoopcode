package parti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMap extends Mapper<IntWritable, Text,IntWritable, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
         context.write(key,value);
        /*
        String line=value.toString();
        String[] wds=line.split("\\s+");
        context.write(new IntWritable(Integer.valueOf(wds[0])),new IntWritable(Integer.valueOf(wds[1])));
        */
    }
}
