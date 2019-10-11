package topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopnReduce extends Reducer<IntWritable,Text,Text,IntWritable> {
    private int count=0;
    private int n;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        n=Integer.valueOf(context.getConfiguration().get("topn"));
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text a:values){
                if(count<n){
                      context.write(a,key);
                      count+=1;
                }else{
                    break;
                }
        }
    }


}
