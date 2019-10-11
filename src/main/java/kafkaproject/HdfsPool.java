package kafkaproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;

import javax.swing.text.html.parser.Entity;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HdfsPool {
    private Map<String, FSDataOutputStream> pool =new HashMap<String,FSDataOutputStream>();

    private static HdfsPool instance ;

    private FileSystem fs=null;

    private HdfsPool(){
        Configuration conf=new Configuration();
        // 更换节点的策略  never 永远不更换
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
        // 客户端在写失败的时候 是否更换datnode
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        try {
            fs=FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public synchronized static HdfsPool getInstance(){
        if(instance==null){
            synchronized(HdfsPool.class) {
                if(instance==null){
                    instance=new HdfsPool();
                }
                return instance;
            }
        }
        return instance;
    }

    public synchronized FSDataOutputStream take(String fileName){
        FSDataOutputStream out=pool.get(fileName);
        if(out!=null){
            pool.remove(fileName);
            return out;
        }
        Path path =new Path(fileName);
        try {
            // 可以使用一个配置参数 来配置主目录 以防止 主目录不存在的情况下 创建文件失败
            if(fs.exists(path)){
                out=fs.append(path);
            }else{

                out=fs.create(path);
            }
            return out;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public void add(String fileName,FSDataOutputStream out){
         pool.put(fileName,out);
    }

    public void release(){
        for(Map.Entry<String,FSDataOutputStream> a:pool.entrySet()){
            try {
                a.getValue().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("pooll is close");
    }
}
