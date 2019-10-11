package bbjoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupss extends WritableComparator {
   protected MyGroupss(){
       super(BBKey.class,true);
   }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        BBKey one=(BBKey)    a;
        BBKey two=(BBKey) b;
        return one.getProductId().compareTo(two.getProductId());
   }
}
