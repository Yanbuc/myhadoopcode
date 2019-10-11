package bbjoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BBKey implements WritableComparable<BBKey> {
    private String productId;
    private int flag;

    public BBKey(){}
    public BBKey(String productId, int flag) {
        this.productId = productId;
        this.flag = flag;
    }

    @Override
    public int compareTo(BBKey o) {
        return productId.compareTo(o.getProductId());
        /*
        if(productId.equals(o.getProductId())) {
            return (flag - o.flag);
        }
        return productId.compareTo(o.getProductId());
        */
    }

    @Override
    public void write(DataOutput out) throws IOException {
           out.writeUTF(productId);
           out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       productId=in.readUTF();
       flag=in.readInt();
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return productId+"_"+flag;
    }
}
