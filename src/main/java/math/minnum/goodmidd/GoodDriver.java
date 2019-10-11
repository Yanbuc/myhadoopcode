package math.minnum.goodmidd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GoodDriver {
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
        Path in =new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(conf);
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(GoodKey.class);
        job.setMapperClass(GoodMap.class);
        job.setJarByClass(GoodDriver.class);
        job.setReducerClass(GoodReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
      //  job.setNumReduceTasks(2);
        job.setCombinerClass(GoodCombiner.class);
        job.waitForCompletion(true);
    }
}
