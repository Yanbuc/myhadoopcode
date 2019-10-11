package ordermaxprice;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import wordcount.ReduCal;

import java.io.IOException;

public class OrderReduce extends Reducer<Text, DoubleWritable, Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double max=0;
        for(DoubleWritable d:values){
            if(max < d.get()){
                max=d.get();
            }
        }
        context.write(key,new DoubleWritable(max));
    }
}
