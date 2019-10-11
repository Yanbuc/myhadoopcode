package hadoop_try;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Map;

public class TestMapWritableFile {
    public static void main(String[] args) throws IOException {
         write();
        //read();
    }

    // 将map
    public static  void write() throws IOException {
        MapWritable a=new MapWritable();
        a.put(new IntWritable(1),new Text("zhangsan"));
        a.put(new IntWritable(2),new Text("lisi"));
        FileOutputStream file=new FileOutputStream("D:/c.txt");
        DataOutputStream out=new DataOutputStream(file);
        a.write(out);
        out.close();
        file.close();
    }
    // 从文件之中读取数值 并且展现出来
    public static  void read() throws IOException {
        FileInputStream in=new FileInputStream("D:/c.txt");
        DataInputStream da=new DataInputStream(in);
        MapWritable c=new MapWritable();
        c.readFields(da);
        for(Map.Entry<Writable,Writable> entry:c.entrySet()){
            System.out.println(entry.getKey()+" : "+entry.getValue());
        }
        da.close();
        in.close();

    }


}
