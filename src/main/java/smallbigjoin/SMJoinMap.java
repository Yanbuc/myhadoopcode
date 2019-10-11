package smallbigjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SMJoinMap extends Mapper<LongWritable,Text,Text, Text> {
    private   Text pid=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split("\\s+");
        pid.set(fields[2]);
        context.write(pid,value);
    }
}
