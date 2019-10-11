package FlowSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private  long downFlow;
    private  long upflow;
    private  long sum;

    public FlowBean(){}
    public FlowBean( long upflow,long downFlow) {
        this.downFlow = downFlow;
        this.upflow = upflow;
        this.sum=downFlow+upflow;
    }

    @Override
    public int compareTo(FlowBean o) {
        return sum-o.sum>0 ? -1:1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
           out.writeLong(upflow);
           out.writeLong(downFlow);
           out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
            upflow=in.readLong();
            downFlow=in.readLong();
            sum=in.readLong();
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return this.upflow+"\t"+this.downFlow+"\t"+this.sum;
    }
}
