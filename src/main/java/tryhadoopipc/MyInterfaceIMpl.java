package tryhadoopipc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyInterfaceIMpl implements MyInterface{
    @Override
    public String hello(String msg) {
        System.out.println(msg);
        return "hello " + msg;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        try {
          return  ProtocolSignature.getProtocolSignature(protocol,clientVersion);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
