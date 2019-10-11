package kafkaproject;

import java.util.TimerTask;

public class ClosePool  extends TimerTask {
    @Override
    public void run() {
        HdfsPool pool=HdfsPool.getInstance();
        pool.release();
    }
}
