package calculateFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CalMap extends Mapper<LongWritable,Text,Text, Text> {

    private Text ftext=new Text();
    private Text utext=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String log=value.toString();
        String[] fields=log.split(":");
        if(fields.length<2){
            return ;
        }
        String user=fields[0];
        String[] friends=fields[1].split(",");
        for(String friend:friends){
            ftext.set(friend);
            utext.set(user);
            context.write(ftext,utext);
        }
    }
}
