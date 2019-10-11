package bbjoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class BBMap extends Mapper<LongWritable,Text,BBKey,Text> {
    private BBKey bbKey=new BBKey();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit file=(FileSplit) context.getInputSplit();
        Path path = file.getPath();
        String productId;
        if(path.getName().contains("sorder")){
            productId=value.toString().split("\\s+")[2];
            bbKey.setFlag(0);
            bbKey.setProductId(productId);
        }else{
            productId=value.toString().split("\\s+")[0];
            bbKey.setProductId(productId);
            bbKey.setFlag(1);

        }
        context.write(bbKey,value);

    }
}
