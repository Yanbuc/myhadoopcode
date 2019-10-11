package smalljoin;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Year;
import java.util.HashMap;
import java.util.Map;

public class SmallReduce extends Reducer<IntWritable,Text, Text, NullWritable> {

    private Map<Integer,String> map=null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf=context.getConfiguration();
        FileSystem fs=FileSystem.get(conf);
       // Path path=new Path(conf.get("smalldir"));
        URI[] cacheFiles = context.getCacheFiles();
        Path path=new Path(cacheFiles[0].toString());
        FSDataInputStream open = fs.open(path);
        BufferedReader reader=new BufferedReader(new InputStreamReader(open));
        String temp="";
        int userId;
        map=new HashMap<Integer, String>();
        while ((temp=reader.readLine())!=null){
            userId=Integer.valueOf(temp.split("\\s+")[0]);
            map.put(userId,temp);
        }
        open.close();
        reader.close();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int userId=key.get();
        String userInfo=map.get(userId);
        if(userInfo==null){
            return ;
        }
        for(Text text:values){
            context.write(new Text(userInfo+"  "+text.toString()),NullWritable.get());
        }
    }
}
