package kafkaproject;

import java.util.Timer;
import java.util.TimerTask;

public class AppConsumer {
    public static void main(String[] args) {

        new Timer().schedule(new ClosePool(),0,30000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                CleanedConsumer consumer=new CleanedConsumer("fk");
                consumer.processTag();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                RawConsumer consumer = new RawConsumer("fk");
                consumer.processTag();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                RawConsumer consumer = new RawConsumer("fk");
                consumer.processTag();
            }
        }).start();

    }
}
