package multifilesoutput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MulMapper extends Mapper<LongWritable, Text,Text,Text> {
    private Text orderId=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String order=value.toString().split("\\s+")[0];
        orderId.set(order);
        context.write(orderId,value);
    }
}
