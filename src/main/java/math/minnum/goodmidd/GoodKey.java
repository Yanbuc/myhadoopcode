package math.minnum.goodmidd;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GoodKey  implements Writable {
    private double rate=0.0;
    private int num=0;

    public  GoodKey(){}

    public GoodKey(double rate, int num) {
        this.rate = rate;
        this.num = num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
         out.writeDouble(rate);
         out.writeInt(num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
           rate=in.readDouble();
           num=in.readInt();
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public int getNum() {
        return num;
    }
}
