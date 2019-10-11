package hadoop_try;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.jboss.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.io.DataOutputStream;
import java.io.IOException;

public class TestSequence {
    public static void main(String[] args) throws IOException {
        Configuration conf =new Configuration();
      //  write(conf);
        readSequenceFile(conf);
    }


    // 往hdfs 上面写入文件 我希望在写的时候能够通过反射的方式来获得文件之中key val 的数据类型
    public  static void write(Configuration conf) throws IOException {
        Path path=new Path("/testssa.seq");
        SequenceFile.Writer.Option option=SequenceFile.Writer.file(path);
        SequenceFile.Writer.Option option1=SequenceFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option option2=SequenceFile.Writer.valueClass(Text.class);
        // 设置压缩的方式是没有压缩
        BZip2Codec codec=new BZip2Codec();
        codec.setConf(conf);
        SequenceFile.Writer.Option option3=SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,codec );
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, option, option1, option2,option3);
        IntWritable key=new IntWritable();
        Text val=new Text();
        for(int i=0;i<10000;i++){
            key.set(i);
            val.set("tom"+i);
            writer.append(key,val);
        }
        writer.close();
        //SequenceFile.createWriter(conf)
    }

    public static void readSequenceFile(Configuration conf) throws IOException {
        Path paths=new Path("/test.seq");
        SequenceFile.Reader.Option path=SequenceFile.Reader.file(paths);
        SequenceFile.Reader reader=new SequenceFile.Reader(conf,path);
        System.out.println("Compression is "+reader.getCompressionCodec());
        System.out.println("codecType is "+reader.getCompressionType());
        System.out.println("key class is "+reader.getKeyClassName());
        System.out.println("val class is "+reader.getValueClassName());
        IntWritable key=new IntWritable();
        Text val=new Text();
        long pos=reader.getPosition();
        reader.sync(0);
        reader.next(key,val);
        System.out.println(key+" "+val);

        reader.close();
    }

}
