package singlecomputer;

import java.util.HashSet;

public class Test {
    public static void main(String[] args) {
        HashSet<String> one=new HashSet<String>();
        one.add("H");
        one.add("A");
        one.add("O");
        for(String a:one){
            System.out.println(a);
        }
    }
}
