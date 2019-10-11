package multifilesoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;

// 指定输出的文件的名字 指定输出的文件的数目 指定输出的文件的目录
// 原本mapreduce的output的文件的输出文件的位置是mapreduce自己指定的，但是现在可以自己设置输出的文件
// 数量 以及输出的文件的名字。 主要是org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
// 源码上面有使用的方法。
public class MulDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job= Job.getInstance(conf);
        job.setJarByClass(MulDriver.class);
        job.setJobName("merge job");
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        job.setMapperClass(MulMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MulReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
        job.setNumReduceTasks(2);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);

    }
}
