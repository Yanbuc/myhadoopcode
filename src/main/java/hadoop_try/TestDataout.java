package hadoop_try;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class TestDataout {
    public static void main(String[] args) throws IOException {
        /*
        ByteArrayOutputStream by=new ByteArrayOutputStream();
        DataOutputStream da=new DataOutputStream(by);
        da.writeUTF("1");
        da.close();
        System.out.println(by.toByteArray().length);
       */
        ser();
    }

    // java 序列化 和hadoop 的序列化的速度以及字节大小的比较
    public static  void ser() throws IOException {
        long startTime=System.nanoTime();
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        DataOutputStream output=new DataOutputStream(out);
        IntWritable a =new IntWritable(100);
        a.write(output);
        LongWritable b=new LongWritable(1000);
        b.write(output);
        Text c=new Text("thankyou");
        c.write(output);
        System.out.println(out.toByteArray().length);
        output.close();
        out.close();
        System.out.println(System.nanoTime()-startTime);

        long st=System.nanoTime();
        ByteArrayOutputStream by=new ByteArrayOutputStream();
        ObjectOutputStream opt=new ObjectOutputStream(by);
        opt.writeObject(new Integer(100));
        opt.writeObject(new Integer(1000));
        opt.writeObject(new String("thankyou"));
        opt.close();
        System.out.println(by.toByteArray().length);
        by.close();
        System.out.println(System.nanoTime()-st);
    }

}
