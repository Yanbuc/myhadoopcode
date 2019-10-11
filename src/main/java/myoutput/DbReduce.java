package myoutput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DbReduce  extends Reducer<Text, IntWritable,MyDBKey,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         String filename=key.toString();
         int count=0;
         for(IntWritable cnt:values){
             count+=cnt.get();
         }
         MyDBKey v=new MyDBKey();
         v.setFilename(filename);
         v.setId(count);
         context.write(v,new IntWritable(1));
    }
}
