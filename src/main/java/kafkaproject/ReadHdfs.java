package kafkaproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class ReadHdfs {
    public static void main(String[] args) throws IOException {
        Configuration conf =new Configuration();
        FileSystem fs =FileSystem.get(conf);
        FSDataInputStream open = fs.open(new Path("/busi/2019_04_17master.log"));
        BufferedReader bf=new BufferedReader(new InputStreamReader(open));
        System.out.println(bf.readLine());
        System.out.println(bf.readLine());
        System.out.println(bf.readLine());
    }
}
