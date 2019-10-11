package wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import java.io.IOException;

public class WordApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<2){
            System.out.println("there is no enough params");
            return ;
        }
        Configuration conf=new Configuration();
     //   conf.set("fs.defaultFS","file:///");
     //   conf.set(MRJobConfig.CLASSPATH_FILES,"/opt/fastjson-1.2.50.jar"); 这个尝试我没有成功。
        JobConf jobConf=new JobConf(conf);
        // 设置文件切片的最大 最小的size
      //  jobConf.setInt(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE,100);
      //  jobConf.setInt(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE,600);
       // jobConf.setJarByClass(WordApp.class);
        Path in=new Path(args[0]);
        Path out=new Path(args[1]);
        FileInputFormat.addInputPath(jobConf,in);
        FileOutputFormat.setOutputPath(jobConf,out);
        Job job= Job.getInstance(jobConf);
        job.setMapperClass(MapCl.class);
        job.setReducerClass(ReduCal.class);
        // 因为没有写输出的类的类型 所以报出了一个类型转换的错误
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJobName("word count");
        job.setJarByClass(WordApp.class);
        //NLineInputFormat.setNumLinesPerSplit(job,20);


        //job.addFileToClassPath(new Path("/fastjson-1.2.50.jar"));
      //  job.setCombinerClass(MyContainer.class);
       // job.setPartitionerClass(MyPartitioner.class); // 设置分区器
      //  job.setNumReduceTasks(2);
        //job.setPartitionerClass();
        job.waitForCompletion(true);

    }
}
