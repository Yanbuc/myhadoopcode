package dbinputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class DbMyApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length<1){
            System.out.println("args length is not enough long");
            return;
        }

        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
        //
        // jobConf.setNumReduceTasks(2);
        Path out=new Path(args[0]);
        // 配置数据库的参数
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/sl","root","root");
        DBConfiguration dh=new DBConfiguration(conf);
        dh.setInputQuery("select id,filename from file ");
        // 获取实例
        Job job= Job.getInstance(conf);
        job.setJarByClass(DbMyApp.class);
        job.setJobName("db input format");
        // 设置输入
       // DBInputFormat.setInput(job,DbFile.class,"file","","","id","filename");
        // 设置DbInputFormat
        job.setInputFormatClass(DBInputFormat.class);
        job.setMapperClass(DbMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job,out);
        job.waitForCompletion(true);
    }
}
