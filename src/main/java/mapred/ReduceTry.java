package mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceTry extends Reducer<Text, IntWritable,Text,IntWritable> {
    // 按照map 之中产生的几个不同的key 来运行多少次reduce
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       // super.reduce(key, values, context);
       int max=Integer.MIN_VALUE;
       int count=0;
       for(IntWritable a:values){
           max=Math.max(max,a.get());
           count++;
       }
       context.getCounter("r",key+" rr"+this.hashCode()).increment(count);
       context.write(key,new IntWritable(max));
    }
}
