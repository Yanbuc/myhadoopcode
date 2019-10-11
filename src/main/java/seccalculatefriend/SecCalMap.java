package seccalculatefriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

// 计算用户朋友的第二个mr作业
public class SecCalMap  extends Mapper<LongWritable,Text,Text, Text> {
    private Text userPair=new Text();
    private Text frend=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String log=value.toString();
        String[] infor=log.split("\t");
        String friend=infor[0];
        frend.set(friend);
        String[] users=infor[1].split(",");
        Arrays.sort(users);
        for(int i=0;i<users.length-1;i++){
            for(int j=i+1;j<users.length;j++){
                userPair.set(users[i]+"_"+users[j]);
                context.write(userPair,frend);
            }
        }
    }
}
