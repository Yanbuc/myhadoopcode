package topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopnMap extends Mapper<LongWritable,Text, IntWritable,Text> {
    private IntWritable k=new IntWritable();
    private Text v=new Text();

    private TreeMap<Integer,String> treeMap=new TreeMap<Integer, String>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String log=value.toString();
        String[] fields=log.split("\\s+");
        if(fields.length<2){
            return ;
        }
        int n=Integer.valueOf(context.getConfiguration().get("topn"));
        treeMap.put(Integer.valueOf(fields[1]),fields[0]);
        if(treeMap.size() > n){
            treeMap.remove(treeMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Integer,String> map:treeMap.entrySet()){
            k.set(map.getKey());
            v.set(map.getValue());
            context.write(k,v);
        }
    }
}
