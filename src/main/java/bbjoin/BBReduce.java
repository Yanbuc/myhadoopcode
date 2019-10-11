package bbjoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BBReduce extends Reducer<BBKey, Text,Text, NullWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("start up");
    }

    @Override
    protected void reduce(BBKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
/*
        for(Text a:values){
            System.out.println(a.toString());
        }
        System.out.println("==================================");
*/

        List<Text> listone=new ArrayList<Text>();
        List<Text> listTwo=new ArrayList<Text>();
        for(Text a:values){
            if(key.getFlag()==0){
                listone.add(new Text(a.toString()));
            }else{
                listTwo.add(new Text(a.toString()));
            }
        }
      //  System.out.println(listone.get(0).toString()+"  "+listone.size()+" "+listTwo.size());
       for(Text a:listone){
           for(Text c:listTwo){
               context.write(new Text(a.toString()+" "+c.toString()),NullWritable.get());
           }
       }


        /*
        Iterator<Text> one=values.iterator();
        String ks=key.toString();
        System.out.println(ks);
        String k1=one.next().toString();
        while (one.hasNext()){
            Text b=one.next();
            context.write(new Text(k1+" "+b.toString()),NullWritable.get());
        }
       */

    }
}
