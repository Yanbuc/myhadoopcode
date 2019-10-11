package trycodec;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.*;


public class TestCodec {
    public static void main(String[] args) throws IOException {
        FileSystem fs;
        DefaultCodec codec=new DefaultCodec();
        FileInputStream in =new FileInputStream("D://a.txt");
        codec.createInputStream(in);
        //FileInputStream a;
        DataOutputStream dataOutputStream=new DataOutputStream(new ByteArrayOutputStream());
    }
}
