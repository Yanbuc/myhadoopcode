package sg;

import java.util.Scanner;

public class TestUser {
    public static void main(String[] args) {
        Scanner in=new Scanner(System.in);
        System.out.println("用户名");
        String pwd=in.nextLine();
        if(pwd.equals("ccc")|| pwd.equals("hhhh")||pwd.equals("oooo")){
            System.out.println("用户名输入正确");
        }else{
            System.out.println("系统没有这个用户");
        }
    }
}
