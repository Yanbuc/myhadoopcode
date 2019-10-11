package seccalculatefriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecCalReduce extends Reducer<Text,Text,Text, Text> {
    private Text fds=new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb=new StringBuffer();
        for(Text text:values){
             sb.append(text.toString()).append(",");
        }
        String frineds=sb.toString();
        frineds=frineds.substring(0,frineds.length()-1);
        fds.set(frineds);
        context.write(key,fds);
    }
}
