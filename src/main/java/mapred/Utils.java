package mapred;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

public class Utils {
    public static  String getGrp(String grp){
        String time=new Date(System.currentTimeMillis()).toString();
        try {
            String retn= InetAddress.getLocalHost().getHostName()+" : "+time+" : "+grp;
             return retn;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }
}
