package sg;

import java.util.Scanner;

public class TestLib {
    public static void main(String[] args) {
        Scanner in=new Scanner(System.in);
        System.out.println("请输入密码");
        String pwd=in.nextLine();
        if(pwd.equals("aaa")){
            System.out.println("密码输入正确");
        }else{
            System.out.println("密码输入错误");
        }
    }
}
