package writemypack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SaveAsSeq {
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        String fileName="/ss."+SequenceFile.getDefaultCompressionType(conf);
        SequenceFile.Writer.Option option=SequenceFile.Writer.file(new Path(fileName));
        SequenceFile.Writer.Option option1=SequenceFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option option2=SequenceFile.Writer.valueClass(Text.class);
        // 主要的使用的思路就是 除了创建一个文件之外，还要传输的参数就是 键的类型 数值的类型，就是这个样子
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, option,option2,option1);
        IntWritable key=new IntWritable();
        Text val=new Text();
        for(int i=0;i<1000;i++){
            key.set(i);
            val.set("tom"+i);
            writer.append(key,val);
        }
        writer.close();
    }
}
