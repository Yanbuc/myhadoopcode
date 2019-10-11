package parti;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// 自定义的分区器
public class MyPartitioner extends Partitioner<IntWritable, IntWritable> {
    @Override
    public int getPartition(IntWritable text, IntWritable intWritable, int numPartitions) {
        int data=text.get();
        if(data<20){
            return 0;
        }else if(data >=20 && data<100){
            return 1;
        }else {
            return 2;
        }
    }
}
