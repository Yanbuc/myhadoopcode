package hadoop_try;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class TestLocalFileSystem {
    public static void main(String[] args) throws URISyntaxException, IOException {
        // 所以在这里的体会是 使用hadoop 的本地文件系统 可以向本地的文件写入数据。
        // 不同的文件系统的实现是不同的
        Configuration conf =new Configuration();
        Path path=new Path("D://a.txt");
        LocalFileSystem localFileSystem =new LocalFileSystem();
        localFileSystem.initialize(new URI("file:///"),conf);
        FSDataOutputStream out = localFileSystem.create(path);
        out.write("hello world".getBytes());
        out.close();
    }
}
