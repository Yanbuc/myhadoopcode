package Flow;

import FlowSort.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMap extends Mapper<LongWritable,Text,FlowBean,Text> {
    private FlowBean flowBean=new FlowBean();
    private Text text=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split("\t");
        long upFlow=Long.valueOf(fields[1]);
        long downFlow=Long.valueOf(fields[2]);
        flowBean.setSum(upFlow+downFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpflow(upFlow);
        text.set(fields[0]);
        context.write(flowBean,text);
    }
}
