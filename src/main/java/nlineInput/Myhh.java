package nlineInput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class Myhh extends Mapper<LongWritable, Text,Text, IntWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val=value.toString();
        String[] wds=val.split("\\s+");
        Text k=new Text();
        IntWritable v=new IntWritable();
        for(String wd:wds){
        k.set(wd);
        v.set(1);
        context.write(k,v);
        }
        }
}
