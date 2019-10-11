package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;


import java.io.FileOutputStream;
import java.io.IOException;

public class MapRedApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("args length is not enough long");
            return;
        }

        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        //
        JobConf jobConf=new JobConf(conf);
       // jobConf.setNumReduceTasks(2);
        Path  in =new Path(args[0]);
        Path out=new Path(args[1]);
        FileInputFormat.addInputPath(jobConf,in);
        FileOutputFormat.setOutputPath(jobConf,out);
         // 将jobConf 传入之前 一定要设置好所有的参数  如果参数再之后设置 但是这样的参数就是会无效的。
        Job job=Job.getInstance(jobConf);
        job.setJarByClass(MapRedApp.class);
        job.setCombinerClass(Com.class);
        job.setJobName("first map reduce");

        job.setMapperClass(MapFIr.class);
        job.setReducerClass(ReduceTry.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
    }
}
