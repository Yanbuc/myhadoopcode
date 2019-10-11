package sg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.HashMap;

public class Test {
    public static void main(String[] args) throws ClassNotFoundException, IOException {
        /*
        String name="org.apache.hadoop.io.compress.Lz4Codec";
        Class<?> clazz=Class.forName(name);
        Configuration conf=new Configuration();
        CompressionCodec codec=(CompressionCodec) ReflectionUtils.newInstance(clazz,conf);
        // 其实CodePool的创建解压器的方法 其实首先从池子之中去取 没有就自己去创建。就是这么简单
        Compressor compressor = CodecPool.getCompressor(codec);
        FileOutputStream out=new FileOutputStream("D:/a.txt");
        FileInputStream in=new FileInputStream("D:/c.txt");
        // 然后运用工厂的方式 其实复用的是Comprossor这个对象。 所以returnCompressor这个方法是很重要的
        CompressionOutputStream outputStream = codec.createOutputStream(out, compressor);
        IOUtils.copyBytes(in,out,1024,false);
        CodecPool.returnCompressor(compressor);

        HashMap<String,Integer> a =new HashMap<String,Integer>();
        a.put("zhansg",0);
        if(a.get("sk")==null){
            System.out.println("nono");
        }

        File file =new File("D:/tt/data.txt");
        BufferedReader bf=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String line=bf.readLine();
        System.out.println(line.split("\\s+").length);
        */
        HashMap<Integer,Integer>a =new HashMap<Integer, Integer>();
        Integer b=a.get(1);
        System.out.println(b);

    }
}
