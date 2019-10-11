package hadoop_try;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class TestHdfs {
    // 使用url的方式从hdfs之中读取数据
    public static  void main(String[] args) throws Exception {
        URL.setURLStreamHandlerFactory( new FsUrlStreamHandlerFactory());
        String url="hdfs://master:9000/one.txt";
        URL url1 = new URL(url);
        URLConnection urlConnection = url1.openConnection();
        InputStream inputStream = urlConnection.getInputStream();
        byte[] buf=new byte[1024];
        int length=0;
        while ((length=inputStream.read(buf))!=-1){
            byte[] tmp=new byte[length];
            for(int i=0;i<length;i++){
                tmp[i]=buf[i];
            }
            System.out.println(new String(tmp));
        }
    }
}
