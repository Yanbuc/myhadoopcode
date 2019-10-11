package hadoop_try;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class TestMapFile {
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        //write(conf);
        read(conf);
    }
    // 将数据以mapFile的形式进行写入。
    public static  void write(Configuration conf) throws IOException {
        Path path =new Path("/today.map");
        // 注意这里的Option 都是SequenceFile 文件之中的类。
        SequenceFile.Writer.Option option= MapFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option option1 = MapFile.Writer.valueClass(Text.class);
        MapFile.Writer writer=new MapFile.Writer(conf,path,option,option1);
        System.out.println(writer.getIndexInterval());
        writer.setIndexInterval(20);
        IntWritable key=new IntWritable();
        Text val=new Text();
        for(int i=0;i<200;i++){
            key.set(i);
            val.set("tom"+i);
            writer.append(key,val);
        }
        writer.close();
    }
     // 读取mapFile之中的文件
    public  static  void read(Configuration conf) throws IOException {
        Path path =new Path("/today.map");
        MapFile.Reader reader=new MapFile.Reader(path,conf);
        // 读取mapFile文件之中全部的数据
        IntWritable key=new IntWritable();
        Text val=new Text();
        /*
        while (reader.next(key,val)){
            System.out.println(key+" :  "+val);
        }
        */
        key.set(100);
        // get 和seek的方式有什么不同？ seek 是跳转到不同的位置
        Writable a= reader.get(key,val);
        System.out.println(a);
        key.set(100);
        if(!reader.seek(key)){
            System.out.println("sorry ");
        }else{
            System.out.println("no no");
        }
        // seek 还有跳转的时候 当选择的时候
        reader.next(key,val);
        System.out.println(key+" : "+val);
        reader.close();
    }

}
