package hadoop_try;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

// 使用hdfs的API来读取hdfs之中的文件内容。
public class TestHdfsByApi {
    public static  void main(String[] args) throws IOException {
        Path path=new Path("hdfs://master:9000/one.txt");
        Configuration conf =new Configuration();
        FileSystem fs =FileSystem.get(conf);
        FSDataInputStream in = fs.open(path);
        IOUtils.copyBytes(in,System.out,1024);
        in.seek(0);
        System.out.println("=====================");
        IOUtils.copyBytes(in,System.out,1024);
    }


        public  static void testH() throws IOException {
            Configuration conf=new Configuration();
            FileSystem fs=FileSystem.get(conf);
            Path path=new Path("hdfs://master:9000/a.txt");
            FSDataOutputStream fsDataOutputStream = fs.create(path);
            byte[] wr;
            String text="write to hdfs";
            wr=text.getBytes();
            fsDataOutputStream.write(wr);
        }



}
