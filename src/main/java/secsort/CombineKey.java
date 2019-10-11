package secsort;

import mapred.Com;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombineKey implements  WritableComparable<CombineKey> {

    int year;
    int temp;

    public CombineKey(){

    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(temp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
         year=in.readInt();
         temp=in.readInt();
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    @Override
    public int compareTo(CombineKey o) {
        if (this.year != o.getYear()) {
            return this.year - o.getYear();
        } else {
                return o.temp - this.temp;
        }
    }

    public CombineKey(int year, int temp) {
        this.year = year;
        this.temp = temp;
    }

    @Override
    public String toString() {
        return year + "  " + temp;
    }
}
