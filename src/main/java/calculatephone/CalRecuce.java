package calculatephone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CalRecuce  extends Reducer<Text,Text,Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int flow=0;
        int upFlow=0;
        int max=0;
        Iterator<Text> iterator = values.iterator();
        Text tmp;
        String[] splits;
        while (iterator.hasNext()){
            tmp=iterator.next();
            splits=tmp.toString().split("\t");
            if(splits.length!=2){
                continue;
            }
            flow+=Integer.valueOf(splits[0]);
            upFlow+=Integer.valueOf(splits[1]);
        }
        max=flow+upFlow;
        context.write(key,new Text(flow+"\t"+upFlow+"\t"+max));
    }
}
