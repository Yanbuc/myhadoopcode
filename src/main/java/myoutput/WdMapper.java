package myoutput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WdMapper  extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line=value.toString();
         String[] wds=line.split("\\s+");
         Text text=new Text();
         IntWritable intWritable=new IntWritable();
         for(String wd:wds){
             text.set(wd);
             intWritable.set(1);
             context.write(text,intWritable);
         }
    }
}
