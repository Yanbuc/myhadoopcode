package dbinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import wordcount.MapCl;

import java.io.IOException;

public class DbMapper extends Mapper<LongWritable,DbFile, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, DbFile value, Context context) throws IOException, InterruptedException {
        String fileName=value.getFileName();
        int id=value.getId();
        context.write(new Text(fileName),new IntWritable(id));
    }
}
