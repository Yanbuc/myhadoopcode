package order2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Order2Reduce extends Reducer<MyDefineKey, NullWritable,MyDefineKey,NullWritable> {
    @Override
    protected void reduce(MyDefineKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
