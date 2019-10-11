package calculateFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CalReuduce extends Reducer<Text,Text,Text,Text> {
    private Text allUser=new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer users=new StringBuffer();
        for(Text one:values){
            users.append(one.toString()).append(",");
        }
        String user=users.toString();
        if(user==null||user==""){
            return ;
        }
        user=user.substring(0,user.length()-1);
        allUser.set(user);
        context.write(key,allUser);
    }
}
