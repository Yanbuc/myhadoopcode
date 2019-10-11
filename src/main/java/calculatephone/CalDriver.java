package calculatephone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import  calculatephone.CalMap;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Job;
import sg.Test;

import java.io.IOException;


public class CalDriver {
    public static  void  main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        JobConf jobConf=new JobConf(conf);
        jobConf.setJarByClass(CalDriver.class);
        jobConf.setJobName("calculate phone");
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        FileInputFormat.addInputPath(jobConf,in);
        FileOutputFormat.setOutputPath(jobConf,out);
        Job job= Job.getInstance(jobConf);
        job.setMapperClass(CalMap.class);
        job.setReducerClass(CalRecuce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
