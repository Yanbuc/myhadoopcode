package sg;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class Readss {
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(conf);
        Path path=new Path("/a.txt");
        FSDataInputStream in = fs.open(path);
        BufferedReader bu=new BufferedReader(new InputStreamReader(in));
        String tmp="";
        while ((tmp=bu.readLine())!=null){
            System.out.println(tmp);
        }
    }
}
