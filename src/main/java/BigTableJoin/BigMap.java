package BigTableJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class BigMap  extends Mapper<LongWritable, Text,BigJoinKey,Text> {
    private int us;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String filename = inputSplit.getPath().toString();
        int us;
        int flag;
        if(filename.contains("user")){
            flag=0;
            us=Integer.valueOf(value.toString().split("\\s+")[0]);
            context.write(new BigJoinKey(us,flag),value);
        }else{
            flag=1;
            us=Integer.valueOf(value.toString().split("\\s+")[2]);
            context.write(new BigJoinKey(us,flag),value);
        }
    }
}
