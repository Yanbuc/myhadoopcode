package SeqInputFormat;


import nlineInput.Myhh;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        JobConf jobConf=new JobConf(conf);
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(jobConf);
        // 设置输入路径 设置输出路径
        SequenceFileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        // 设置格式化的类
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // 设置使用的map的类
        job.setMapperClass(Myhs.class);
        // 设置map的输入 输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJarByClass(WordApp.class);
        job.setJobName("seqss");
        job.waitForCompletion(true);
    }
}
