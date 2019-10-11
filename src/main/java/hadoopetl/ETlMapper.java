package hadoopetl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETlMapper extends Mapper<LongWritable,Text, Text, NullWritable> {
    Text val=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str=EtlUtils.getETLString(value.toString());
        if(str==null){
            return ;
        }
        val.set(str);
        context.write(val,NullWritable.get());
    }
}
