package wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// 这个其实就是在map端实现了Combiner 提升了效率。
public class MapCl extends Mapper<LongWritable, Text,Text, IntWritable> {

    // 字典
    private HashMap<String,Integer> directory=null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException { super.setup(context);
       // 当map进程被成立的时候，在内存之中建立一个容器，用来存储这个map数据处理的中间数据结果。
       directory=new HashMap<String, Integer>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString().trim();
        String[] words=line.split("\\s+");
        if(directory.size()>20000){
            // 这里的话 就是为了防止写入内存的数据的量实在是太大，然后就是等到数据的
            // 量 到了一定的级别之后，就是将数据写入到流之中。
            writeDataAndClear(context);
        }
        if(line.equals("")|| words.length==0 || line==null){
            return ;
        }
        for(String wd:words){
           if(directory.get(wd)==null){
               directory.put(wd,1);
           }else{
               int count=directory.get(wd);
               directory.put(wd,count+1);
           }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text key=new Text();
        IntWritable val=new IntWritable();
        for(Map.Entry entry:directory.entrySet()){
            key.set((String) entry.getKey());
            val.set((Integer) entry.getValue());
            context.write(key,val);
        }
    }
    private void writeDataAndClear(Context context) throws IOException, InterruptedException {
        Text key=new Text();
        IntWritable val=new IntWritable();
        for(Map.Entry entry:directory.entrySet()){
            key.set((String) entry.getKey());
            val.set((Integer) entry.getValue());
            context.write(key,val);
        }
        directory=new HashMap<String, Integer>();
    }

}
