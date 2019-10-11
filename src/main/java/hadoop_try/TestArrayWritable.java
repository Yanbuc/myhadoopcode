package hadoop_try;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class TestArrayWritable {
    public static void main(String[] args) throws IOException {
        // 问题 如何使用这个ArrayWritable数组？
        /*
        ArrayWritable b =new ArrayWritable(IntWritable.class);
        IntWritable[] c=new IntWritable[10];
        for(int i=0;i<10;i++){
                c[i]=new IntWritable(i);
        }
        b.set(c);
        FileOutputStream file=new FileOutputStream("D:/b.txt");
        DataOutputStream da=new DataOutputStream(file);
        b.write(da);
        da.close();
        file.close();
        */
        reaFromFile();
    }

    public static  void reaFromFile() throws IOException {
        FileInputStream file=new FileInputStream("D:/b.txt");
        DataInputStream data=new DataInputStream(file);
        ArrayWritable b=new ArrayWritable(IntWritable.class);
        b.readFields(data);
        Writable[] v =b.get();
        for(int i=0;i<10;i++){
            System.out.println(v[i].toString());
        }
        data.close();
        file.close();
    }
}
