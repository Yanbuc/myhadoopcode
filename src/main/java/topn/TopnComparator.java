package topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopnComparator extends WritableComparator{

   protected   TopnComparator(){
        super(IntWritable.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable one=(IntWritable) a;
        IntWritable two=(IntWritable) b;
       return -(one.compareTo(two));
    }
}
