package ordermaxprice;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMap  extends Mapper<LongWritable, Text,Text, DoubleWritable> {
    private Text text=new Text();
    private DoubleWritable dou=new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split("\\s+");
        text.set(fields[1]);
        dou.set(Double.valueOf(fields[2]));
        context.write(text,dou);
    }
}
