package multifilesoutput;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class MulReduce extends Reducer<Text,Text, Text, NullWritable> {
   // private MultipleTextOutputFormat<Text,NullWritable> mul=new MultipleTextOutputFormat<Text, NullWritable>();
    private MultipleOutputs<Text,NullWritable> mul=null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mul=new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileName=key.toString();
        for(Text a:values){
            mul.write(a,NullWritable.get(),fileName);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mul.close();
    }
}
