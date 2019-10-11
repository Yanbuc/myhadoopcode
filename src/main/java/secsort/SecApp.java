package secsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SecApp {
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

        job.setMapperClass(SecMap.class);
        job.setReducerClass(SecReduce.class);

        job.setOutputKeyClass(CombineKey.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJobName("word count");
        job.setJarByClass(SecMap.class);
        job.setGroupingComparatorClass(SecGroup.class);
        job.setPartitionerClass(SecPartitioner.class);
        job.waitForCompletion(true);

       // job.setCacheArchives();
    }
}
