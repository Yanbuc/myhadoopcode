package ordermaxprice;

import datadistinct.DisMap;
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

// 对于取最大的话 最重要的是排序 只有排序才能取最大的。
// 在mapreduce之中 排序有两种方式  在reduce端进行排序
// 自定义key 进行全局排序
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        JobConf jobConf=new JobConf(conf);
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(jobConf);
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);

        job.setMapperClass(OrderMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(OrderReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setJarByClass(OrderDriver.class);
        job.waitForCompletion(true);
    }
}
