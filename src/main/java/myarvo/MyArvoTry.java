package myarvo;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class MyArvoTry {
    public static void main(String[] args) throws IOException {
        // 创建arvo 生成的类的第一种方法
        // 创建arvo 生成的类的第二种方法
        User user = User.newBuilder().setFavoriteColor("yellow").setName("wangwu").setFavoriteNumber(12).build();

        // 串行化 user 到磁盘 但是这里的具体的API的意思还是不是很明白
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user.getSchema(), new File("D://users.avro"));
        dataFileWriter.append(user);
        dataFileWriter.close();
        // 反序列化
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>( new File("D://users.avro"), userDatumReader);
        User users = null;
        while (dataFileReader.hasNext()) {
            users = dataFileReader.next(users);
            System.out.println(users);
        }

    }
}
