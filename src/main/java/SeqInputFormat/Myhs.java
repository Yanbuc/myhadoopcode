package SeqInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Myhs extends Mapper<IntWritable, Text,Text, IntWritable> {
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
         context.write(value,new IntWritable(1));
    }
}
