package kafkaproject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.Field;

import java.nio.file.Files;


public class Test {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        // 更换节点的策略  never 永远不更换
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
        // 客户端在写失败的时候 是否更换datnode
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileSystem fs = FileSystem.get(conf);
                    Path p =new Path("/uiu.txt");
                    FSDataOutputStream out=null;
                    if(fs.exists(p)){
                        out=fs.append(p);
                    }else{
                        out=fs.create(p);
                    }
                    while (true){
                        out.write("aaa".getBytes());
                        out.write("\n".getBytes());
                        out.hflush();
                    }
                } catch (IOException e) {
                    System.out.println("one create");
                    e.printStackTrace();
                }
            }
        }).start();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FileSystem fs = FileSystem.get(conf);
                    Path p =new Path("/uiu.txt");
                    FSDataOutputStream out=null;
                    if(fs.exists(p)){
                        out=fs.append(p);
                    }else{
                        out=fs.create(p);
                    }
                    while (true){
                        out.write("aaa".getBytes());
                        out.write("\n".getBytes());
                        out.hflush();
                    }
                } catch (IOException e) {
                    System.out.println("two append ");
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
