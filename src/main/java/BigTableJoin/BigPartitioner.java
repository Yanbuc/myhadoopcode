package BigTableJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BigPartitioner extends Partitioner<BigJoinKey, Text> {
    @Override
    public int getPartition(BigJoinKey bigJoinKey, Text text, int numPartitions) {
        return bigJoinKey.getUserId() % numPartitions;
    }
}
