package smalljoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wordcount.MapCl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

// 大表与小表进行连接
public class SmallDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
      //  conf.set("smalldir","");
        conf.set("fs.defaultFS","file:///");
        JobConf jobConf=new JobConf(conf);
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        Job job= Job.getInstance(jobConf);
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);

        job.setMapperClass(SmallMap.class);
        job.setReducerClass(SmallReduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        // 因为没有写输出的类的类型 所以报出了一个类型转换的错误
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJobName("word count");
        job.setJarByClass(SmallDriver.class);
        job.setNumReduceTasks(2);
        job.addCacheFile(new URI("file:///D:/tt/user.txt"));
        job.waitForCompletion(true);
    }
}
