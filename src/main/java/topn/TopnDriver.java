package topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TopnDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, IOException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        //  conf.set("smalldir","");
        conf.set("fs.defaultFS","file:///");
        conf.set("remoteMap","file:///D:/tt/products.txt");
        conf.set("topn","5");
        JobConf jobConf=new JobConf(conf);
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(jobConf);
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);

        job.setMapperClass(TopnMap.class);
        job.setReducerClass(TopnReduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJobName("smjoin");
        job.setSortComparatorClass(TopnComparator.class);
        job.setJarByClass(TopnDriver.class);
        job.waitForCompletion(true);

    }
}
