package FlowSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow=0;
        long downFlow=0;
        for(FlowBean flowBean:values){
            upFlow+=flowBean.getUpflow();
            downFlow+=flowBean.getDownFlow();
        }
        FlowBean bean=new FlowBean(upFlow,downFlow);
        context.write(key,bean);
    }
}
