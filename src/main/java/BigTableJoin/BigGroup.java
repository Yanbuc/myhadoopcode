package BigTableJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BigGroup extends WritableComparator {

    public BigGroup() {
        super(BigJoinKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        BigJoinKey one=(BigJoinKey)a;
        BigJoinKey two=(BigJoinKey)b;
        return one.getUserId()-two.getUserId();
    }

}
