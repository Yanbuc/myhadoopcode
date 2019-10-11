package secsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecGroup extends WritableComparator {
    protected SecGroup(){
        super(CombineKey.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombineKey one=(CombineKey) a;
        CombineKey two=(CombineKey) b;
        return one.getYear() == two.getYear() ? 0 : -1;
    }
}
