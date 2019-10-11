package bbjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<BBKey, Text> {
    @Override
    public int getPartition(BBKey bbKey, Text text, int numPartitions) {
        return bbKey.getProductId().hashCode() % numPartitions;
    }
}
