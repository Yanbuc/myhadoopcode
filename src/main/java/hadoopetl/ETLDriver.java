package hadoopetl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.temporal.Temporal;

public class ETLDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            return ;
        }
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Configuration conf=new Configuration();
        //conf.set("fs.defaultFS","file:///");
        Job job=Job.getInstance(conf);
        job.setJarByClass(ETLDriver.class);
        job.setJobName("ETL job");
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        job.setMapperClass(ETlMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.waitForCompletion(true);
    }
}
