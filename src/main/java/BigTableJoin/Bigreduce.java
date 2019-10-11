package BigTableJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import secsort.CombineKey;

import java.io.IOException;
import java.util.Iterator;

public class Bigreduce extends Reducer<BigJoinKey, Text, Text, NullWritable> {
    @Override
    protected void reduce(BigJoinKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        String user=it.next().toString();
        Text next;
        while (it.hasNext()){
            next=it.next();
            System.out.println(user);
            context.write(new Text(user+" "+next.toString()),NullWritable.get());
        }
    }
}
