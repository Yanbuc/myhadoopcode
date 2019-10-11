package tryhadoopipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyClient {
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","file:///");
       /*
          创建客户端的方法 左边客户端的类型 可以是定义的协议接口。
          但是最后能够获得这样的一个类型的对象 中间的逻辑是什么样的呢？
          我传入的是接口的类型 而且这个类型是我自定义的，那么这个类到底是什么？
        */

        MyInterface client=  RPC.getProxy(MyInterface.class,MyInterface.versionID,new InetSocketAddress("localhost",8888),conf);
        System.out.println(client.hello("world "));
    }
}
