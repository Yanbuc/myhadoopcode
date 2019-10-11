package order2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyDefineKey  implements WritableComparable<MyDefineKey> {
    private String categoryId;
    private double price;
    public  MyDefineKey(){};
    public MyDefineKey(String categoryId, double price) {
        this.categoryId = categoryId;
        this.price = price;
    }

    @Override
    public int compareTo(MyDefineKey o) {
        return price>o.getPrice() ? -1:1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
         out.writeUTF(categoryId);
         out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        categoryId=in.readUTF();
        price=in.readDouble();
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return categoryId+"\t"+price;
    }
}
