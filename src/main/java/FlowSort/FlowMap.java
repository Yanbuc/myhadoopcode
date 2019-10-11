package FlowSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMap extends Mapper<LongWritable, Text,Text,FlowBean> {

    private  FlowBean flowBean=new FlowBean();
    private Text phoneNumber=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split("\t");
        context.getCounter("a","as").increment(1);
        //System.out.println(fields.length);
        if(fields.length<=2){
            return ;
        }
        String phone=fields[1];
        long downFlow=Long.valueOf(fields[fields.length-2]);
        long upFLow=Long.valueOf(fields[fields.length-3]);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpflow(upFLow);
        flowBean.setSum(downFlow+upFLow);
        phoneNumber.set(phone);
        context.write(phoneNumber,flowBean);
    }
}
