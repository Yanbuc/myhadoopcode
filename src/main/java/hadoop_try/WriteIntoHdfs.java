package hadoop_try;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class WriteIntoHdfs {
    public static void main(String[] args) throws IOException {
        Configuration conf =new Configuration();
        Path path =new Path("/one.txt");
        FileSystem fs =FileSystem.get(conf);
        FSDataOutputStream out = fs.create(path);
        out.write("hello world".getBytes());
        out.hflush();
        out.write("say hello".getBytes());
    }
}
