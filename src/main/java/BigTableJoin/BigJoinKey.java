package BigTableJoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigJoinKey implements WritableComparable<BigJoinKey> {
    private int userId;
    private int flag;

    public BigJoinKey() {
    }

    public BigJoinKey(int userId, int flag) {
        this.userId = userId;
        this.flag = flag;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public int compareTo(BigJoinKey o) {
        if(userId!=o.getUserId()){
            return userId-o.getUserId();
        }
        return flag-o.getFlag() ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
         out.writeInt(userId);
         out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
         userId=in.readInt();
         flag=in.readInt();
    }

    @Override
    public String toString() {
        return userId+ "_"+flag;
    }
}
