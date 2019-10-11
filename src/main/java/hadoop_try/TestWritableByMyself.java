package hadoop_try;

import java.io.*;

public class TestWritableByMyself {

    public static void main(String[] args) throws IOException {
      // 测试自定义的串行化的类 并将其反序列化
        Person person=new Person("shangxia","jiangshu",12);
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        DataOutputStream output=new DataOutputStream(out);
        person.write(output);
        Person b=new Person();
        out.close();
        ByteArrayInputStream in=new ByteArrayInputStream(out.toByteArray());
        out.close();
        DataInputStream ins=new DataInputStream(in);
        b.readFields(ins);
        System.out.println(b);
    }
}
