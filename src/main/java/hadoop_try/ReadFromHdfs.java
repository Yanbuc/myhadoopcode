package hadoop_try;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


import java.io.IOException;


public class ReadFromHdfs {
    public static void main(String[] args) throws IOException {
        Configuration conf =new Configuration();
        FileSystem fs =FileSystem.get(conf);
        Path path= new Path("hdfs://192.168.249.10:9000/a.txt");
        FileStatus fileStatus=fs.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for(BlockLocation a:fileBlockLocations){
            System.out.println(a.toString());
        }
    }
}
