package hadoop_try;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ReadFromHar {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf=new Configuration();
        HarFileSystem fs=new HarFileSystem();
        // 这里的路径要延申到har文件夹
        fs.initialize(new URI("har:////har/dir.har"),conf);
        Path path =new Path( "./");
        FileStatus[] fss = fs.listStatus(path);
        for(FileStatus fsss:fss){
            System.out.println(fsss.toString());
        }

        /*
        FileSystem fs=HarFileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(path);
        FileSystem fs =FileSystem.get(conf);

        for(FileStatus fss:fileStatuses){
            System.out.println(fss.toString());
        }
        */
    }
}
