package smallbigjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class SMJoinReduce extends Reducer<Text, Text,Text, NullWritable> {
    private HashMap<String,String> map=null;
    private Text text=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map=new HashMap<String, String>();
        Configuration conf=context.getConfiguration();
        FileSystem fs= FileSystem.get(conf);
        String filepath=conf.get("remoteMap");
        BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(new Path(filepath))));
        String tmp="";
        String[] fields;
        while ((tmp=reader.readLine())!=null){
            fields=tmp.split("\\s+");
            if(fields.length<3){
                continue;
            }
            map.put(fields[0],fields[1]+" "+fields[2]);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String product=map.get(key.toString());
        if(product==null ||product==""){
            System.out.println(product);
            return ;
        }
        String tmp;
        for(Text a:values){
            tmp=a.toString()+" "+product;
            text.set(tmp);
            context.write(text,NullWritable.get());
        }
    }
}
