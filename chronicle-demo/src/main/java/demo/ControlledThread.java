package demo;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility class adding some pause exit methods to a Thread
 */
public abstract class ControlledThread extends Thread {
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private AtomicBoolean isExit = new AtomicBoolean(false);

    public void run() {
        while (true) {
            if(isExit.get()){
                cleanup();
                return;
            }
            if (isRunning.get()) {
                loop();
            } else {
                sleepnx(100);
            }
        }
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
