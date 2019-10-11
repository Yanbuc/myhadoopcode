package calculatephone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CalMap extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val=value.toString();
        String[] splits=val.split("\t");
        if(splits.length<3){
            return ;
        }
        Text phone=new Text(splits[0]);
        Text va=new Text(splits[1]+"\t"+splits[2]);
        context.write(phone,va);
    }
}
