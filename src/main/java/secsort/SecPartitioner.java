package secsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecPartitioner  extends Partitioner<CombineKey, NullWritable> {
    @Override
    public int getPartition(CombineKey combineKey, NullWritable nullWritable, int numPartitions) {
        return combineKey.getYear() % numPartitions;
    }
}
