package seccalculatefriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SecCalDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        JobConf jobConf=new JobConf(conf);
        jobConf.setJarByClass(SecCalDriver.class);
        jobConf.setJobName("calculate phone");
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(jobConf);
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        job.setMapperClass(SecCalMap.class);
        job.setReducerClass(SecCalReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
