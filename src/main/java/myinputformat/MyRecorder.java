package myinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



import java.io.IOException;


// 两个泛型的参数是什么意思 key 是传入的参数值 val 是读取的数据
public class MyRecorder  extends RecordReader<NullWritable, BytesWritable> {

    private BytesWritable val;
    private boolean isProcessed=false;
    private FileSplit file=null;
    private Configuration conf;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        file=(FileSplit) split;
        conf=context.getConfiguration();
    }

    public MyRecorder() {
        super();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
       if(!isProcessed){
           Path a =file.getPath();
           FileSystem fs =a.getFileSystem(conf);
           FSDataInputStream open = fs.open(a);
           byte[] data=new byte[(int)file.getLength()];
           System.out.println(file.getPath()+"  "+file.getLength());
           open.read(data);
           val=new BytesWritable(data);
           isProcessed=true;
           return true;
           // return true;
       }
       return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isProcessed==true ? 1.0f:0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
