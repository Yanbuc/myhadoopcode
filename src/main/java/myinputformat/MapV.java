package myinputformat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapV  extends Mapper<NullWritable, BytesWritable,Text, IntWritable> {
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        byte[] data=value.copyBytes();
        String passage=new String(data).replace("\n"," ");
        String[] wds=passage.split("\\s+");
        Text k=new Text();
        IntWritable v=new IntWritable();
        for(String wd:wds){
            k.set(wd);
            v.set(1);
            context.write(k,v);
        }
    }
}
