package order2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMap2 extends Mapper<LongWritable, Text,MyDefineKey, NullWritable> {
    private  MyDefineKey defineKey=new MyDefineKey();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String log=value.toString();
          String[] fields=log.split("\\s+");
          defineKey.setCategoryId(fields[1]);
          defineKey.setPrice(Double.valueOf(fields[2]));
          context.write(defineKey,NullWritable.get());
    }
}
