package secsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecReduce extends Reducer<CombineKey, NullWritable,CombineKey,NullWritable> {
    @Override
    protected void reduce(CombineKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

          // 但是对这个有点不理解，尤其是这里。
          for(NullWritable c:values){
              System.out.println(key.getYear()+" : "+key.getTemp());
              context.write(key,c);
          }
    }
}
