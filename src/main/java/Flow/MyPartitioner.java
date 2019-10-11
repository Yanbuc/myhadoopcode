package Flow;

import FlowSort.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner  extends Partitioner<FlowBean, Text> {
    private int distance=500;

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        int[] dis=new int[numPartitions];
        long sum=flowBean.getSum();
        for(int i=0;i<numPartitions-1;i++){
            dis[i]=(i+1)*distance;
            if(sum <dis[i]){
                return numPartitions-1-i;
            }
        }

        return 0;
    }
}
