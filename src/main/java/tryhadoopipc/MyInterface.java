package tryhadoopipc;

import org.apache.hadoop.ipc.VersionedProtocol;

// 但是为什么要定义这个接口呢？ 需要继续听下去 继承了VersionedProtocol的好处是什么呢？
// 为什么继承了这个接口 就是可以实现RPC了？
public interface MyInterface  extends VersionedProtocol {
    public static final long versionID = 10L;
    public String hello(String msg);
}
