package math.minnum.goodmidd;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GoodCombiner extends Reducer<IntWritable, GoodKey,IntWritable,GoodKey> {

    private Map<Double,Integer> container=new HashMap<Double, Integer>();
    private GoodKey goodKey=new GoodKey();
    @Override
    protected void reduce(IntWritable key, Iterable<GoodKey> values, Context context) throws IOException, InterruptedException {
         container.clear();
         for(GoodKey one:values){
             if( container.get(one.getRate()) == null){
                container.put(one.getRate(),1);
             }else{
                 int n=container.get(one.getRate());
                container.put(one.getRate(),n+1);
             }
         }
         for(Map.Entry<Double,Integer> a:container.entrySet()){
             goodKey.setNum(a.getValue());
             goodKey.setRate(a.getKey());
             context.write(key,goodKey);
         }
    }
}
