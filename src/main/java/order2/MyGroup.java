package order2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
    protected MyGroup(){
        super(MyDefineKey.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MyDefineKey one=(MyDefineKey) a;
        MyDefineKey two=(MyDefineKey) b;
        return one.getCategoryId().equals(two.getCategoryId()) ? 0 : 1;
    }
}
