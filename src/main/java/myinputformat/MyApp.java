package myinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class MyApp   {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        // jobConf.setJarByClass(WordApp.class);
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(conf);
       // MyInputFormat.setMaxInputSplitSize(job,3000);
       // MyInputFormat.setMinInputSplitSize(job,6000);
        job.setJobName("my input format");
        job.setJarByClass(MyApp.class);
        job.setMapperClass(MapV.class);
        job.setInputFormatClass(MyInputFormat.class);
         MyInputFormat.addInputPath(job,in);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(IntWritable.class);
         FileOutputFormat.setOutputPath(job,out);

        job.waitForCompletion(true);
    }
}
