package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyContainer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int i=0;
        int count=0;
        for(IntWritable a:values){
            i+=a.get();
            count+=1;
        }
        context.getCounter("com","rr"+this.hashCode()).increment(count);
        context.write(key,new IntWritable(i));
    }
}
