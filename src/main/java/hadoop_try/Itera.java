package hadoop_try;

import java.io.File;

public class Itera {
    public static void main(String[] args) {
        File a =new File("D:/a");
        Itera b =new Itera();
        b.readFile(a);
    }

    // 遍历文件夹之中的文件
    public void readFile(File a){
        if(a.isDirectory()){
            System.out.println(a.getName());
            for(File b:a.listFiles()){
                readFile(b);
            }
        }else{
            System.out.println(a.getName());
        }
    }

}
