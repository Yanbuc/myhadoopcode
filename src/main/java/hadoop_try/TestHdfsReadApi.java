package hadoop_try;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;


public class TestHdfsReadApi {
    public  static  void  main(String[] args) throws IOException {
        Path path=new Path("hdfs://master:9000/directory");
        Configuration conf =new Configuration();
        FileSystem fs = FileSystem.get(conf);

         FsStatus status = fs.getStatus(path);
        FileStatus[] fileStatuses = fs.listStatus(path);
        for(FileStatus aa:fileStatuses){
            System.out.println(aa.getPath());
        }

        // 使用文件系统来判断文件是否是存在的 fs.exists(path)
        // TestHdfsReadApi a =new TestHdfsReadApi();
        // a.print(fs,fs.getFileStatus(path));
        //RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles();
    }
    public void print( FileSystem fs,FileStatus fs0) throws IOException {
        System.out.println(fs0.getPath().getName());
        if(fs0.isDirectory()){
            Path p=fs0.getPath();
            FileStatus[] lists = fs.listStatus(p);
            for(FileStatus f:lists){
                print(fs,f);
            }
        }

    }

}
