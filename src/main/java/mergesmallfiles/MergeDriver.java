package mergesmallfiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
// 其实小文件的合并 实现的思路 其实就是将文件之中的数据 合并到一个文件之中罢了。
// 之后在集群上面尝试下 我怕会出现权限的问题。
public class MergeDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job= Job.getInstance(conf);
        job.setJarByClass(MergeDriver.class);
        job.setJobName("merge job");
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        FileInputFormat.addInputPath(job,in);
        SequenceFileOutputFormat.setOutputPath(job,out);
        job.setMapperClass(MergeMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(true);
        FileSystem fs=FileSystem.get(conf);
        fs.delete(in,true);
    }
}
