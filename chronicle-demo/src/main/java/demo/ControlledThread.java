package demo;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility class adding some pause exit methods to a Thread
 */
public abstract class ControlledThread extends Thread {
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private AtomicBoolean isExit = new AtomicBoolean(false);
    private int loopTime = 0;

    public void run() {
        while (true) {
            if(isExit.get()){
                cleanup();
                return;
            }
            if (isRunning.get()) {
                if(loopTime!=0)sleepnx(loopTime);
                loop();
            } else {
                sleepnx(200);
            }
        }
    }

    public void setLoopTime(int loopTimeMS){
        loopTime = loopTimeMS;
    }
    public abstract void loop();

    public abstract void cleanup();

    public void pause() {
        isRunning.set(false);
    }

    public void go() {
        isRunning.set(true);
    }

    public void exit() {
        isExit.set(true);
    }

    public void sleepnx(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
