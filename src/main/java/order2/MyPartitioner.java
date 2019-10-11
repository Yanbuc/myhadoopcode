package order2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyDefineKey, NullWritable> {
    @Override
    public int getPartition(MyDefineKey myDefineKey, NullWritable nullWritable, int numPartitions) {
        return myDefineKey.getCategoryId().hashCode() % numPartitions;
    }
}
