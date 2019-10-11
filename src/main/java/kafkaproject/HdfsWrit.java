package kafkaproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;


public class HdfsWrit {
    private FileSystem fs =null;
    private HdfsPool pool =null;
    private String prefile="";
    private FSDataOutputStream out =null;

    public  HdfsWrit(){
        Configuration conf=new Configuration();
        // 更换节点的策略  never 永远不更换
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
         // 客户端在写失败的时候 是否更换datnode
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        try {
            fs= FileSystem.get(conf);
            pool=HdfsPool.getInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 将消息写入文件之中
    public void write(String filePath,String message) {
        if(!filePath.equals(prefile)) {
            if (out == null) {
                out = pool.take(filePath);
            } else {
                pool.add(prefile, out);
                out = pool.take(filePath);
            }
            prefile = filePath;
        }
        try {
            out.write(message.getBytes());
            out.write("\n".getBytes());
            out.hflush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
