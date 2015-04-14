/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
