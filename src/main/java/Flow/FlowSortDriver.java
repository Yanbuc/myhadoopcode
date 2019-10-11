package Flow;

import FlowSort.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSortDriver {
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

        job.setMapperClass(FlowSortMap.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FlowSortReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setJarByClass(FlowSortDriver.class);
        job.setNumReduceTasks(3);
        job.waitForCompletion(true);
        FileSystem fs=FileSystem.get(conf);
        fs.delete(in,true);
    }
}
