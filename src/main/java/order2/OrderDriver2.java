package order2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 我觉得排序 使用map端全局排序 反而是复杂地多，在reduce端进行排序是非常简单的。
public class OrderDriver2 {
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

        job.setMapperClass(OrderMap2.class);
        job.setMapOutputKeyClass(MyDefineKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(Order2Reduce.class);
        job.setOutputKeyClass(MyDefineKey.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroup.class);
        job.setNumReduceTasks(3);
        job.setJarByClass(OrderDriver2.class);
        job.waitForCompletion(true);
    }
}
