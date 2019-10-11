package myoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MyApps {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Job job=Job.getInstance(conf);
        job.setJarByClass(MyApps.class);
        job.setJobName("outputFormat");
        job.setMapperClass(WdMapper.class);
        FileInputFormat.addInputPath(job,in);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(DbReduce.class);
        job.setOutputKeyClass(MyDBKey.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job,"file","id","filename");
        DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/sl","root","root");
        job.waitForCompletion(true);
        /*
        FileInputFormat.addInputPath(job,in);
      //  SequenceFileOutputFormat.setCompressOutput(job,true);
      //  SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
     //   SequenceFileOutputFormat.setOutputPath(job,out);
     //   SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
     //   FileOutputFormat.setOutputPath(job,out);
      //  FileOutputFormat.setCompressOutput(job,true);
       // FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
        job.setJarByClass(MyApps.class);
        job.setJobName("outputFormat");
        job.setMapperClass(WdMapper.class);
        job.setReducerClass(Recu.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
       // job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(true);
        */
    }
}
