package hadoop_try;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

public class Testtest {
    public void say(){
        System.out.println("hashahh");
        read();
        tre();
    }
    public  void read(){
        System.out.println("read and read");
        System.out.println("read and read two");
    }
    public void tre(){
        System.out.println("okokok");
    }

    public static void main(String[] args) {
        Testtest a =new Testtest();
        a.say();
    }
}
