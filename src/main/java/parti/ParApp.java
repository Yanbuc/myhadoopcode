package parti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import wordcount.WordApp;

import java.io.IOException;

public class ParApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Path path=new Path("file:///D:/tt/par");
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        TotalOrderPartitioner.setPartitionFile(conf,path);
        JobConf jobConf=new JobConf(conf);
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(jobConf);
        SequenceFileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyRed.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setJobName("word count");
        job.setJarByClass(WordApp.class);
        InputSampler.RandomSampler<IntWritable, NullWritable> sampler=new InputSampler.RandomSampler<IntWritable, NullWritable>(0.1,10,3);
        job.setNumReduceTasks(3);
        InputSampler.writePartitionFile(job,sampler);
        job.waitForCompletion(true);
    }
}
