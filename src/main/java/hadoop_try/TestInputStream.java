package hadoop_try;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class TestInputStream {
    public static void main(String[] args) throws IOException {
        seriable();
    }
    // 这里的序列化 是将IntWritable 序列化成为一个字节数组
    public static void seriable() throws IOException {
        IntWritable one=new IntWritable(255);
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        DataOutputStream dataout=new DataOutputStream(out);

        // xuyao genxia
     //   ObjectOutputStream outs=new ObjectOutputStream(out);
        one.write(dataout);
        System.out.println(out.toByteArray().length);
        /*
        IntWritable b=new IntWritable();
        ByteArrayInputStream in=new ByteArrayInputStream(out.toByteArray());
        DataInputStream datain=new DataInputStream(in);
        b.readFields(datain);
        System.out.println(b.get());
        */
    }

    public static  void deSeriable(){
        OutputStream outputStream;
        InputStream in;
        IntWritable b=new IntWritable();
        ObjectOutputStream obj;
        ObjectInputStream jk;
        DataOutput dataOutput;
        Writable writable;
        Text a;
    }

}
