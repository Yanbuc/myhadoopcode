package tryhadoopipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

// 使用hadoop的rpc 实现一个server
// 在创建Server的时候，需要设置 协议 协议的实现类 绑定地址
public class MyServer {
    public static  void main(String[] args) throws IOException {
        Configuration conf =new Configuration();
        conf.set("fs.defaultFS","file:///");
        /* 首先创建的服务器的类别，是RPC类的内部的Server类
           这个是不能弄错的，然后是创建server需要的参数
           创建完成server之后 直接进行start() 就是可以了。
           创建Server方法使用的Builder模式
           要设置的参数 就是要传入的参数  要遵守的协议
           协议的实现类 服务器的地址 服务器的端口 服务器的处理器，然后是一个build()方法。
           写到这里的时候 我忽然有点理解了协议。事先定义了一个协议，
           协议之中定义了一些方法，因为协议是两边都是必须要遵守的。
           然后就是一个具体的实现类。
           接口之中的方法就是协议能做什么。
           然后实现类就是具体的实现规则。
        */
        RPC.Server server  = new RPC.Builder(conf).setInstance(new MyInterfaceIMpl())
                .setProtocol(MyInterface.class).setBindAddress("localhost")
                .setPort(8888).setNumHandlers(2).build();
        server.start();
    }
}
