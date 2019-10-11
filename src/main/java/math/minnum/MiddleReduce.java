package math.minnum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MiddleReduce  extends Reducer<IntWritable,DoubleWritable,IntWritable,Text> {

    private List<Double> rates=new ArrayList<Double>();
    private double jum=0.0;
    private Text out=new Text();
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        rates.clear();
        double sum=0.0;
        double avg=0.0;
        jum=0.0;
        int nn=0;
        for(DoubleWritable one:values){
            rates.add(one.get());
            sum+=one.get();
            nn+=1;
        }
        avg=sum / rates.size();
        if(rates.size()==0){
            return ;
        }
        Collections.sort(rates);
        double num=0.0;
        int size=0;
        if(rates.size()%2==0){
            size=rates.size()/2;
            num=( (rates.get(size)+rates.get(size-1)) ) / 2;
        }else{
            size=rates.size()/2;
            System.out.println(size+"  "+rates.get(size));
            num= rates.get(size);
        }
        for(double x:rates){
            jum+=(x-avg)*(x-avg);
        }
        if(key.get()==1){
            System.out.println(rates.size());
            System.out.println(size-1);
        }
        jum=Math.sqrt(jum);
        out.set(num+" "+jum);
        context.write(key,out);
    }
}
