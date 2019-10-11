package secsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecMap extends Mapper<LongWritable, Text,CombineKey, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         // context.getCacheFiles();
        String line=value.toString();
        int year=Integer.valueOf(line.split("\\s+")[0]);
        int temp=Integer.valueOf(line.split("\\s+")[1]);
        context.write(new CombineKey(year,temp),NullWritable.get());
    }
}
