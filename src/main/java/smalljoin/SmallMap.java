package smalljoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SmallMap extends Mapper<LongWritable, Text, IntWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        System.out.println(line.substring(0,line.length()-1));
        int userId=Integer.valueOf(line.split("\\s+")[2]);
        context.write(new IntWritable(userId),value);
    }
}
