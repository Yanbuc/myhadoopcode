package hadoop_try;

import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

public class TestText {
    public static void main(String[] args) throws IOException {
        Text text=new Text("s中fhhh");
        byte[] c=text.copyBytes();

        System.out.println(text.decode(c));
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        DataOutputStream dataOutputStream=new DataOutputStream(out);
        dataOutputStream.write(100);
        dataOutputStream.writeBytes("hello");
        dataOutputStream.writeBytes("sdfg");
        dataOutputStream.close();
        System.out.println(out.toByteArray().length);
        //System.out.println(c.length);
        //System.out.println(text.getBytes().length);
        //System.out.println(text.getLength());
        // 获得位置为1的字符 但是返回的是字符的整数值
        /*
        System.out.println((char)text.charAt(1));
        System.out.println(text.find("s"));
        System.out.println(text.getLength());
        String s="s中fhhh";
        byte[] bytes =text.getBytes();
        System.out.println(bytes.length);
        */
    }
}
