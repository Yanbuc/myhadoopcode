package math.minnum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 当统计的数据只有一个的时候 均方差为0
// 这个计算中位数的方法 是消耗内存的。
public class MiddleDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("args length is not enough long");
            return;
        }

        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        //
        JobConf jobConf=new JobConf(conf);
        // jobConf.setNumReduceTasks(2);
        Path in =new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(conf);
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(MiddleMap.class);
        job.setJarByClass(MiddleDriver.class);
        job.setReducerClass(MiddleReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(2);
        job.waitForCompletion(true);
    }
}
