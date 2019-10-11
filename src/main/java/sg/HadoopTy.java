package sg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HadoopTy {
    public static void main(String[] args) throws IOException {
        Configuration conf =new Configuration();
        FileSystem fs =FileSystem.get(conf);
        Path path=new Path("hdfs://192.168.249.10:9000/say.txt");
        FSDataOutputStream output = fs.create(path);
        output.write("hello world".getBytes());
        output.flush();
        output.close();
    }
}
