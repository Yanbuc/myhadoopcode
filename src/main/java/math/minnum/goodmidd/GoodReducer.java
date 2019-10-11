package math.minnum.goodmidd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class GoodReducer extends Reducer<IntWritable,GoodKey, IntWritable, Text> {
    private Map<Double,Integer> rates=new TreeMap<Double,Integer>();
    private Text aps=new Text();
    @Override
    protected void reduce(IntWritable key, Iterable<GoodKey> values, Context context) throws IOException, InterruptedException {
        rates.clear();
        int k=key.get();
        int size=0;
        double all=0.0;
        for(GoodKey a:values){
            if(rates.get(a.getRate()) == null){
                rates.put(a.getRate(),a.getNum());
            }else{
                int num=rates.get(a.getRate());
                rates.put(a.getRate(),a.getNum()+num);
            }
            if(k==2){
                System.out.println(a.getNum());
            }
            size=size+a.getNum();
            all+=a.getNum()*a.getRate();
        }
        if(key.get()==2){
           for(Map.Entry<Double,Integer> o:rates.entrySet()){
                System.out.println(o.getKey()+" "+ o.getValue());
            }
            System.out.println(size);
        }

        int tmp=0;
        int midposition=0;
        double vv=0;
        if(size%2==0){
            int flag=0;
            midposition=size/2-1;
            for(Map.Entry<Double,Integer> o:rates.entrySet()){
                tmp+=o.getValue();
               if(tmp> midposition ){
                   for(int i=flag;i<2;i++){
                            vv+=o.getKey();
                            flag+=1;
                   }
                    if(flag==2){
                        break;
                    }
               }else if(tmp == midposition){
                   vv+=o.getKey();
                   flag+=1;
                   if(flag==2){
                       break;
                   }
               }
            }
            vv=vv/2;
        }else{
            midposition=size/2;
            for(Map.Entry<Double,Integer> o:rates.entrySet()) {
                tmp+=o.getValue();
                if(tmp>=midposition){
                    vv=o.getKey();
                    break;
                }
            }
        }
        double avg=all/size;
        double sp=0.0;
        for(Map.Entry<Double,Integer> o:rates.entrySet()){
            for(int i=0;i<o.getValue();i++){
                sp+=(avg-o.getKey())*(avg-o.getKey());
            }
        }
        sp= Math.sqrt(sp);
        aps.set(vv+" "+sp);
        context.write(key,aps);
    }
}
