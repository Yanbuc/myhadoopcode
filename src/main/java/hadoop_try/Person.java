package hadoop_try;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Person implements WritableComparable<Person> {
    private String name;
    private String address;
    private  int age;

    public Person(String name,String address,int age){
        this.name=name;
        this.address=address;
        this.age=age;
    }
    public Person(){

    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(address);
        out.writeInt(age);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name=in.readUTF();
        address=in.readUTF();
        age=in.readInt();
    }

    @Override
    public int compareTo(Person o) {
        if(this.age > o.age){
            return 1;
        }else{
            return -1;
        }
    }
    public String toString(){
        return name+" "+address+"  "+age;
    }
}
